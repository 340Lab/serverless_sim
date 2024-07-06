# Copyright 2022 The Deep RL Zoo Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""
From the paper "Curiosity-driven Exploration by Self-supervised Prediction"
https://arxiv.org/abs/1705.05363

From the paper "Proximal Policy Optimization Algorithms"
https://arxiv.org/abs/1707.06347.
"""
from absl import app
from absl import flags
from absl import logging
import os

os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'

import multiprocessing
import numpy as np
import torch
import copy


# pylint: disable=import-error
from deep_rl_zoo.networks.policy import ActorCriticConvNet
from deep_rl_zoo.networks.curiosity import IcmNatureConvNet
from deep_rl_zoo.ppo_icm import agent
from deep_rl_zoo.checkpoint import PyTorchCheckpoint
from deep_rl_zoo.schedule import LinearSchedule
from deep_rl_zoo import main_loop
# from deep_rl_zoo import gym_env
from deep_rl_zoo import greedy_actors
from proxy_env3 import ProxyEnv3

FLAGS = flags.FLAGS
flags.DEFINE_string('environment_name', 'ServerlessSim', 'Atari name without NoFrameskip and version, like Breakout, Pong, Seaquest.')
# flags.DEFINE_integer('environment_height', 84, 'Environment frame screen height.')
# flags.DEFINE_integer('environment_width', 84, 'Environment frame screen width.')
# flags.DEFINE_integer('environment_frame_skip', 4, 'Number of frames to skip.')
# flags.DEFINE_integer('environment_frame_stack', 4, 'Number of frames to stack.')
flags.DEFINE_integer('num_actors', 1, 'Number of worker processes to use.')
flags.DEFINE_bool('clip_grad', False, 'Clip gradients, default off.')
flags.DEFINE_float('max_grad_norm', 10.0, 'Max gradients norm when do gradients clip.')
flags.DEFINE_float('learning_rate', 0.00035, 'Learning rate.')
flags.DEFINE_float('icm_learning_rate', 0.00015, 'Learning rate for ICM module.')

flags.DEFINE_float('discount', 0.99, 'Discount rate.')
flags.DEFINE_float('gae_lambda', 0.95, 'Lambda for the GAE general advantage estimator.')
flags.DEFINE_float('entropy_coef', 0.001, 'Coefficient for the entropy loss.')
flags.DEFINE_float('value_coef', 0.5, 'Coefficient for the state-value loss.')
flags.DEFINE_float('clip_epsilon_begin_value', 0.12, 'PPO clip epsilon begin value.')
flags.DEFINE_float('clip_epsilon_end_value', 0.02, 'PPO clip epsilon final value.')

flags.DEFINE_float('intrinsic_lambda', 0.1, 'Scaling factor for intrinsic reward when calculate using equation 6.')
flags.DEFINE_float('icm_beta', 0.2, 'Weights inverse model loss against the forward model loss in ICM module.')
flags.DEFINE_float('policy_loss_coef', 1.0, 'Weights policy loss against the the ICM module loss.')

flags.DEFINE_integer('unroll_length', 128, 'Collect N transitions (cross episodes) before send to learner, per actor.')
flags.DEFINE_integer('update_k', 4, 'Run update k times when do learning.')
flags.DEFINE_integer('num_iterations', 100, 'Number of iterations to run.')
flags.DEFINE_integer(
    'num_train_steps', int(5e5), 'Number of training steps (environment steps or frames) to run per iteration, per actor.'
)
flags.DEFINE_integer(
    'num_eval_steps', int(2e4), 'Number of evaluation steps (environment steps or frames) to run per iteration.'
)
flags.DEFINE_integer('max_episode_steps', 108000, 'Maximum steps (before frame skip) per episode.')
flags.DEFINE_integer('seed', 1, 'Runtime seed.')
flags.DEFINE_bool('use_tensorboard', True, 'Use Tensorboard to monitor statistics, default on.')
flags.DEFINE_bool('actors_on_gpu', True, 'Run actors on GPU, default on.')
flags.DEFINE_integer(
    'debug_screenshots_interval',
    0,
    'Take screenshots every N episodes and log to Tensorboard, default 0 no screenshots.',
)
flags.DEFINE_string('tag', '', 'Add tag to Tensorboard log file.')
flags.DEFINE_string('results_csv_path', './logs/ppo_icm_atari_results.csv', 'Path for CSV log file.')
flags.DEFINE_string('checkpoint_dir', './checkpoints', 'Path for checkpoint directory.')

class EnvWrapper:
    spec = type('', (object,), {"id": "ServerlessSim"})()
    state_dim = (1,84,84) #9+10
    action_dim = 10

    score=0
    ep=0
    stepcnt=0

    def step(self,action):
        a,b,c,d = self.env.rl_step(action)
        padded_state = np.zeros(self.state_dim)
        padded_state.flat[:21] = a
        a = padded_state#torch.from_numpy(empty_state).float().unsqueeze(0)
        # res[0] 2 state_dim
        self.score+=b
        self.stepcnt+=1
        return a,b,c,d 
    def reset(self):
        if self.stepcnt>0:
            print("score:",self.score, ", avgscore:",self.score/self.stepcnt,", ep:",self.ep)
        self.score=0
        self.stepcnt=0
        self.ep+=1

        self.env=ProxyEnv3()
        
        self.env.config["mech"]["mech_type"]['scale_sche_joint']=''
        self.env.config["mech"]["scale_num"]['rela']=''
        self.env.config["mech"]["scale_down_exec"]['default']=''
        self.env.config["mech"]["scale_up_exec"]['least_task']=''
        self.env.config["mech"]["sche"]['pos']='greedy'
        self.env.config["mech"]["filter"]['careful_down']=''
        self.env.config["mech"]["instance_cache_policy"]['no_evict']=''
        # print("env config:",self.env.config)
        self.env.reset()
        self.env.start_async_sim()
        return np.zeros(self.state_dim) 
        #torch.from_numpy(np.zeros(self.state_dim)).float().unsqueeze(0)

def main(argv):
    """Trains PPO-ICM agent on Atari."""
    del argv
    runtime_device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    logging.info(f'Runs PPO-ICM agent on {runtime_device}')
    np.random.seed(FLAGS.seed)
    torch.manual_seed(FLAGS.seed)
    if torch.backends.cudnn.enabled:
        torch.backends.cudnn.benchmark = False
        torch.backends.cudnn.deterministic = True

    random_state = np.random.RandomState(FLAGS.seed)  # pylint: disable=no-member

    actor_envs = [EnvWrapper()]
    state_dim=actor_envs[0].state_dim
    action_dim=actor_envs[0].action_dim
    empty_state = np.zeros(state_dim)

    logging.info('Environment: %s', FLAGS.environment_name)
    logging.info('Action spec: %s', action_dim)
    logging.info('Observation spec: %s', state_dim)

    # Create policy network, master will optimize this network
    policy_network = ActorCriticConvNet(state_dim=state_dim, action_dim=action_dim)
    policy_optimizer = torch.optim.Adam(policy_network.parameters(), lr=FLAGS.learning_rate)

    # ICM module
    icm_network = IcmNatureConvNet(state_dim=state_dim, action_dim=action_dim)
    icm_optimizer = torch.optim.Adam(icm_network.parameters(), lr=FLAGS.icm_learning_rate)

    # Test network output.
    s = torch.from_numpy(empty_state).float().unsqueeze(0)
    network_output = policy_network(s)
    assert network_output.pi_logits.shape == (1, action_dim)
    assert network_output.value.shape == (1, 1)

    clip_epsilon_scheduler = LinearSchedule(
        begin_t=0,
        end_t=int(
            (FLAGS.num_iterations * int(FLAGS.num_train_steps * FLAGS.num_actors)) / FLAGS.unroll_length
        ),  # Learner step_t is often faster than worker
        begin_value=FLAGS.clip_epsilon_begin_value,
        end_value=FLAGS.clip_epsilon_end_value,
    )

    # Create queue to shared transitions between actors and learner
    data_queue = multiprocessing.Queue(maxsize=FLAGS.num_actors)
    # Create shared objects so all actor processes can access them
    manager = multiprocessing.Manager()

    # Store copy of latest parameters of the neural network in a shared dictionary, so actors can later access it
    shared_params = manager.dict({'policy_network': None})

    # Create PPO-ICM learner agent instance
    learner_agent = agent.Learner(
        policy_network=policy_network,
        policy_optimizer=policy_optimizer,
        icm_network=icm_network,
        icm_optimizer=icm_optimizer,
        clip_epsilon=clip_epsilon_scheduler,
        discount=FLAGS.discount,
        gae_lambda=FLAGS.gae_lambda,
        total_unroll_length=int(FLAGS.unroll_length * FLAGS.num_actors),
        update_k=FLAGS.update_k,
        entropy_coef=FLAGS.entropy_coef,
        value_coef=FLAGS.value_coef,
        intrinsic_lambda=FLAGS.intrinsic_lambda,
        icm_beta=FLAGS.icm_beta,
        policy_loss_coef=FLAGS.policy_loss_coef,
        clip_grad=FLAGS.clip_grad,
        max_grad_norm=FLAGS.max_grad_norm,
        device=runtime_device,
        shared_params=shared_params,
    )

    # Create actor environments, runtime devices, and actor instances.
    
    actor_devices = ['cpu'] * FLAGS.num_actors
    # Evenly distribute the actors to all available GPUs
    if torch.cuda.is_available() and FLAGS.actors_on_gpu:
        num_gpus = torch.cuda.device_count()
        actor_devices = [torch.device(f'cuda:{i % num_gpus}') for i in range(FLAGS.num_actors)]

    actors = [
        agent.Actor(
            rank=i,
            data_queue=data_queue,
            policy_network=copy.deepcopy(policy_network),
            unroll_length=FLAGS.unroll_length,
            device=actor_devices[i],
            shared_params=shared_params,
        )
        for i in range(FLAGS.num_actors)
    ]
    # Setup checkpoint.
    checkpoint = PyTorchCheckpoint(
        environment_name=FLAGS.environment_name, agent_name='PPO-ICM', save_dir=FLAGS.checkpoint_dir
    )
    checkpoint.register_pair(('policy_network', policy_network))
    checkpoint.register_pair(('icm_network', icm_network))

    # Run parallel training N iterations.
    main_loop.run_parallel_training_iterations(
        num_iterations=FLAGS.num_iterations,
        num_train_steps=FLAGS.num_train_steps,
        num_eval_steps=FLAGS.num_eval_steps,
        learner_agent=learner_agent,
        actors=actors,
        actor_envs=actor_envs,
        data_queue=data_queue,
        checkpoint=checkpoint,
        csv_file=FLAGS.results_csv_path,
        use_tensorboard=FLAGS.use_tensorboard,
        tag=FLAGS.tag,
        debug_screenshots_interval=FLAGS.debug_screenshots_interval,
    )


if __name__ == '__main__':
    # Set multiprocessing start mode
    multiprocessing.set_start_method('spawn')
    app.run(main)
