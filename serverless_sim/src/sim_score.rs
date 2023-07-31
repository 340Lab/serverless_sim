impl SimEnv {
    fn state(&mut self) -> String {
        // 关系图
        //  - reqs:{
        //      - req_id:
        //      - dag_id:
        // .    - dag_fns:{fn_id:,node:}  dag_fn 与 node 的映射关系
        //      - done_fns:
        //    }
        //  - dags:{
        //      - dag_id：
        //      - dag图
        // .  }
        //  - node2node_speed: {node_a,node_b,speed}
        //  - nodes:{
        //        cpu:mem,
        // .  }
    }

    /// req_done_avg 平均每个请求处理完的时间 越低越好
    fn score_req_done_avg(&self) -> f32 {
        self.requests
            .iter()
            .map(|(_req_id, req)| req.done_time - req.start_time)
            .sum::<f32>()
            / self.requests.len() as f32
    }

    /// req_done_std 平均每个请求处理完的时间的标准差 越低越好
    fn score_req_done_std(&self) -> f32 {
        let avg = self.score_req_done_avg();
        let sum = self
            .requests
            .iter()
            .map(|(_req_id, req)| (req.done_time - req.start_time - avg).powi(2))
            .sum::<f32>();
        (sum / self.requests.len() as f32).sqrt()
    }

    /// req_done_90 90%的请求处理完的时间 越低越好
    fn score_req_done_90p(&self) -> f32 {
        let mut req_done_times = self
            .requests
            .iter()
            .filter(|(_req_id, req)| req.is_done())
            .map(|(_req_id, req)| req.done_time - req.start_time)
            .collect::<Vec<f32>>();
        req_done_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let req_done_90p_cnt = req_done_times.len() * 0.9 as usize;
        req_done_times[0..req_done_90p_cnt].iter().sum::<f32>() / req_done_90p_cnt as f32
    }

    /// req_move_on_avg 平均每个请求处理任务推进量
    fn score_req_move_on_avg(&mut self) -> f32 {
        self.requests
            .iter()
            .map(|(_req_id, req)| req.cur_frame_done.len())
            .sum::<f32>()
            / self.requests.len() as f32
    }

    fn score(&mut self) -> f32 {
        self.req_done_avg() + self.req_done_std() + self.req_done_90p() + self.req_move_on_avg()
    }
}
