use crate::sim_env::SimEnv;

impl SimEnv {
    /// req_done_avg 平均每个请求处理完的时间 越低越好
    pub fn req_done_time_avg(&self) -> f32 {
        if self.done_requests.borrow().len() == 0
        //  &&
        // self.requests.borrow().len() == 0
        {
            return 0.0;
        }

        let sum = self
            .done_requests
            .borrow()
            .iter()
            // .filter(|req| {
            //     if req.is_done(self) {
            //         assert!(
            //             req.end_frame >= req.begin_frame,
            //             "end_frame should > begin_frame"
            //         );
            //     }
            //     req.is_done(self)
            // })
            .map(|req| (req.end_frame - req.begin_frame) as f32)
            .sum::<f32>();
        // sum += self.requests
        //     .borrow()
        //     .iter()
        //     .map(|req| (self.current_frame() - req.1.begin_frame) as f32)
        //     .sum::<f32>();

        sum / (
            self.done_requests.borrow().len() as f32
            //  + self.requests.borrow().len()
        )
    }

    /// req_done_std 平均每个请求处理完的时间的标准差 越低越好
    pub fn req_done_time_std(&self) -> f32 {
        if self.done_requests.borrow().len() == 0 {
            return 0.0;
        }

        let avg = self.req_done_time_avg();
        let sum = self
            .done_requests
            .borrow()
            .iter()
            // .filter(|req| req.is_done(self))
            .map(|req| (((req.end_frame - req.begin_frame) as f32) - avg).powi(2))
            .sum::<f32>();
        (sum / (self.done_requests.borrow().len() as f32)).sqrt()
    }

    /// req_done_90 90%的请求处理完的时间 越低越好
    pub fn req_done_time_avg_90p(&self) -> f32 {
        let mut req_done_times = self
            .done_requests
            .borrow()
            .iter()
            // .filter(|req| req.is_done(self))
            .map(|req| (req.end_frame - req.begin_frame) as f32)
            .collect::<Vec<f32>>();
        req_done_times.sort_by(|a, b| a.partial_cmp(b).expect("can't cmp f32"));
        let req_done_90p_cnt = req_done_times.len() * (0.9 as usize);
        if req_done_90p_cnt == 0 {
            return 0.0;
        }
        req_done_times[0..req_done_90p_cnt].iter().sum::<f32>() / (req_done_90p_cnt as f32)
    }

    // /// req_move_on_avg 平均每个请求处理任务推进量
    // fn score_req_move_on_avg(&self) -> f32 {
    //     if self.requests.borrow().len() == 0 {
    //         return 0.0;
    //     }
    //     self.requests
    //         .borrow()
    //         .iter()
    //         .map(|(_req_id, req)| req.cur_frame_done.len() as f32)
    //         .sum::<f32>() / (self.requests.borrow().len() as f32)
    // }

    // fn node_avg_mem(&self) -> f32 {
    //     self.nodes
    //         .borrow()
    //         .iter()
    //         .map(|node| node.mem)
    //         .sum::<f32>() / (self.nodes.borrow().len() as f32)
    // }

    pub fn cost_each_req(&self) -> f32 {
        if self.done_requests.borrow().len() == 0 {
            return 0.0;
        }
        *self.cost.borrow() / (self.done_requests.borrow().len() as f32)
    }

    pub fn cost_perform(&self) -> f32 {
        let cost = self.cost_each_req();
        if cost < 0.0001 {
            return 0.0;
        }
        let req_avg_time = self.req_done_time_avg();
        if req_avg_time < 0.0001 {
            return 0.0;
        }

        1.0 / req_avg_time / cost
    }

    pub fn score(&self) -> f32 {
        // let req_done_time_avg = self.req_done_time_avg();
        // let req_done_time_std = self.req_done_time_std();
        // let req_done_time_avg_90p = self.req_done_time_avg_90p();
        // let score_req_move_on_avg = self.score_req_move_on_avg();
        // let node_avg_mem = self.node_avg_mem();
        // log::info!("score consist of req_done_time_avg:{}, req_done_time_std:{}, req_done_time_avg_90p:{}, score_req_move_on_avg:{}, node_avg_mem:{}",
        //     req_done_time_avg, req_done_time_std, req_done_time_avg_90p, score_req_move_on_avg, node_avg_mem);
        let mut score = 0.0;
        // if self.req_done_time_avg() > 6.5 {
        //     score -= 100.0 * self.req_done_time_avg();
        // } else if self.req_done_time_avg() > 6.2 {
        //     score -= 10.0 * self.req_done_time_avg();
        // } else {
        //     score -= self.req_done_time_avg();
        // }
        // if self.cost_each_req() > 1.7 {
        //     score -= self.cost_each_req() * 100.0;
        // } else if self.cost_each_req() > 1.5 {
        //     score -= self.cost_each_req() * 10.0;
        // } else {
        //     score -= self.cost_each_req();
        // }
        if self.config.es.fit_hpa.is_some() {
            panic!("not support fit_hpa");
            // score = -(*self.distance2hpa.borrow() as f32);
        } else {
            if self.config.es.no_perform_cost_rate_score.is_none() {
                score += self.cost_perform();
            }
            score -= self.req_done_time_avg();
        }
        //  -
        // (
        //     self.requests
        //         .borrow()
        //         .iter()
        //         .map(|(_, r)| { self.current_frame() - r.begin_frame })
        //         .sum::<usize>() as f32
        // )

        // Don't left too much requests
        // score -= self.requests.borrow().len() as f32;

        //-self.req_done_time_avg() //越小越好
        // - self.req_done_time_std()//越小越好
        // - self.req_done_time_avg_90p()//越小越好
        // + self.score_req_move_on_avg() // 越大越好
        // - self.node_avg_mem()/500.0 // 越小越好
        // - self.cost_each_req() // 越小越好
        // + self.cost_perform()*10.0
        score
    }
}
