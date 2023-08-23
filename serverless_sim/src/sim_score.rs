use crate::sim_env::SimEnv;

impl SimEnv {
    /// req_done_avg 平均每个请求处理完的时间 越低越好
    pub fn req_done_time_avg(&self) -> f32 {
        if self.done_requests.borrow().len() == 0 {
            return 0.0;
        }

        self.done_requests
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
            .sum::<f32>()
            / self.done_requests.borrow().len() as f32
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
            .map(|req| ((req.end_frame - req.begin_frame) as f32 - avg).powi(2))
            .sum::<f32>();
        (sum / self.done_requests.borrow().len() as f32).sqrt()
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
        let req_done_90p_cnt = req_done_times.len() * 0.9 as usize;
        if req_done_90p_cnt == 0 {
            return 0.0;
        }
        req_done_times[0..req_done_90p_cnt].iter().sum::<f32>() / req_done_90p_cnt as f32
    }

    /// req_move_on_avg 平均每个请求处理任务推进量
    fn score_req_move_on_avg(&self) -> f32 {
        if self.requests.borrow().len() == 0 {
            return 0.0;
        }
        self.requests
            .borrow()
            .iter()
            .map(|(_req_id, req)| req.cur_frame_done.len() as f32)
            .sum::<f32>()
            / self.requests.borrow().len() as f32
    }

    fn node_avg_mem(&self) -> f32 {
        self.nodes.borrow().iter().map(|node| node.mem).sum::<f32>()
            / self.nodes.borrow().len() as f32
    }

    pub fn score(&self) -> f32 {
        // let req_done_time_avg = self.req_done_time_avg();
        // let req_done_time_std = self.req_done_time_std();
        // let req_done_time_avg_90p = self.req_done_time_avg_90p();
        // let score_req_move_on_avg = self.score_req_move_on_avg();
        // let node_avg_mem = self.node_avg_mem();
        // log::info!("score consist of req_done_time_avg:{}, req_done_time_std:{}, req_done_time_avg_90p:{}, score_req_move_on_avg:{}, node_avg_mem:{}",
        //     req_done_time_avg, req_done_time_std, req_done_time_avg_90p, score_req_move_on_avg, node_avg_mem);
        - self.req_done_time_avg()//越小越好
            - self.req_done_time_std()//越小越好
            - self.req_done_time_avg_90p()//越小越好
            + self.score_req_move_on_avg() // 越大越好
            - self.node_avg_mem()/500.0 // 越小越好
            + 1.0 / *self.cost.borrow_mut() // 越小越好
    }
}
