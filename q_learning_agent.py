import numpy as np
import random
from environment import Env
from collections import defaultdict


class QLearningAgent:

    def __init__(self, actions):
        # actions = [0, 1, 2, 3]
        self.actions = actions

        # 设置QLearning需要更新的超参数
        # 学习率
        self.learning_rate = 0.01
        # 折扣率 [0,1)的常数，趋近于0表示agent主要考虑immediate reward，趋近于1表示agent主要考虑future reward
        self.discount_factor = 0.9
        # 探索率/贪婪系数
        self.epsilon = 0.1

        # Q为动作效用函数（action-utility function），用于评价在特定状态下采取某个动作的优劣。它是智能体的记忆。
        self.q_table = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])

    # 采样 <s, a, r, s'> <当前状态，当前动作，下一个状态，下一个动作>
    # 通过bellman方程求解马尔科夫决策过程的最佳决策序列
    def learn(self, state, action, reward, next_state):
        current_q = self.q_table[state][action] #Q是一张表，行记录状态state，列记录动作action
        # 贝尔曼方程更新
        new_q = reward + self.discount_factor * max(self.q_table[next_state])
        self.q_table[state][action] += self.learning_rate * (new_q - current_q)

    # 从Q-table中选取动作
    def get_action(self, state):
        # 如果当前随机数是小于我们的epsilon时
        if np.random.rand() < self.epsilon:
            # 随机移动：贪婪策略随机探索动作action
            action = np.random.choice(self.actions)
        else:
            # 从q表中选择
            state_action = self.q_table[state]
            action = self.arg_max(state_action)
        return action

    @staticmethod
    def arg_max(state_action):
        max_index_list = []
        max_value = state_action[0]
        for index, value in enumerate(state_action):
            if value > max_value:
                max_index_list.clear()
                max_value = value
                max_index_list.append(index)
            elif value == max_value:
                max_index_list.append(index)
        return random.choice(max_index_list)


if __name__ == "__main__":
    env = Env()
    agent = QLearningAgent(actions=list(range(env.n_actions)))
    num_episode = 1000 #迭代次数

    ##=================开始Qlearning算法（游戏）=====================##
    for episode in range(num_episode):

        ## 重置环境初始状态
        state = env.reset()  #重置初始状态
        while True:
            env.render() #更新可视化环境
            # agent基于当前的state选择动作
            action = agent.get_action(str(state))
            # 与环境互动，把动作放到env.step()函数，并返回下一状态，奖励，done
            next_state, reward, done = env.step(action)
            # RL 从这个序列 (state, action, reward, state_) 中学习
            # 更新Q表
            agent.learn(str(state), action, reward, str(next_state))
            # 把下一状态赋值给s，准备开始下一步。
            state = next_state
            env.print_value_all(agent.q_table)
            # 如果已经到达最终状态，就跳出for循环。(开始下一次迭代)
            if done:
                break
