use std::collections::HashSet;
use std::path::Path;
use std::io;
use std::fs::File;
use csv::ReaderBuilder;


use crate::sim_env::SimEnv;

use crate::sim_env::{self, SimEnv};

#[derive(Debug)]
pub struct TaskInfo {
    pub task_name: String,
    pub job_name: String,
    pub dependencies: Vec<u32>,
    pub task_id: u32,
}

pub fn parse_dag_csv(sim_env: &SimEnv) -> io::Result<Vec<TaskInfo>> {
    let file_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/dag_parsers/filtered_tasks.csv");
    let file = File::open(file_path.clone())?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    // 第一次遍历：用随机种子随机选一个 job_name
    let rng = sim_env.env_rand_f(0.0, 1.0);

    // 将 HashSet 转换为 Vec，方便索引操作
    let job_names: HashSet<String> = rdr
        .records()
        .filter_map(|result| result.ok()) // 跳过无效行
        .map(|record| record[1].to_string()) // 提取 job_name
        .collect();  // 去重，得到 job_name 的集合

    let job_names_vec: Vec<String> = job_names.into_iter().collect();  // 将 HashSet 转换为 Vec

    // 根据 rng_value 获取一个随机索引，确保索引在合法范围内
    let selected_index = (rng * (job_names_vec.len() as f32)) as usize;

    // 通过索引从 job_names_vec 中选择 job_name
    let selected_job_name = job_names_vec[selected_index].clone();

    println!("job_name:{}", selected_job_name);

    let file = File::open(file_path)?; // 重新打开文件以重置读取器
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    let mut tasks = Vec::new();

    for result in rdr.records() {
        let record = result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        if record[1] == selected_job_name {
            let task_name = record[0].to_string();

            // 提取 task_id 和 dependencies
            let mut parts = task_name.split('_');
            let task_id = parts
                .next()
                .and_then(|num| num.chars().filter(|c| c.is_digit(10)).collect::<String>().parse().ok())
                .unwrap_or_else(|| {
                    panic!("Failed to parse task_id from task_name: {}", task_name);
                });

            let mut dependencies: Vec<u32> = parts.filter_map(|num| num.parse::<u32>().ok()).collect();
            dependencies.sort();

            tasks.push(TaskInfo {
                task_name,
                job_name: selected_job_name.clone(),
                dependencies,
                task_id,
            });
        }
    }

    tasks.sort_by_key(|task| task.task_id);

    Ok(tasks)
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::path::Path;

//     #[test]
//     fn test_parse_dag_csv() {
//         // 调用 parse_dag_csv 来解析该文件
//         let tasks = parse_dag_csv().expect("Failed to parse the CSV file");

//         // 输出解析的结果到终端
//         println!("Parsed tasks: {:#?}", tasks); // 使用 {:#?} 可以更好地格式化输出

//     }
// }
