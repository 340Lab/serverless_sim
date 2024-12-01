use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io;
use std::fs::File;
use csv::ReaderBuilder;
use rand::seq::IteratorRandom;

#[derive(Debug)]
pub struct TaskInfo {
    pub task_name: String,
    pub job_name: String,
    pub dependencies: Vec<u32>,
    pub task_id: u32,
}

pub fn parse_dag_csv() -> io::Result<Vec<TaskInfo>> {
    let file_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/dag_parsers/filtered_tasks.csv");
    let file = File::open(file_path.clone())?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    // 第一次遍历：随机选一个 job_name
    let mut rng = rand::thread_rng();
    let selected_job_name = rdr
        .records()
        .filter_map(|result| result.ok()) // 跳过无效行
        .map(|record| record[1].to_string()) // 提取 job_name
        .collect::<HashSet<_>>() // 去重
        .into_iter()
        .choose(&mut rng) // 随机选一个
        .expect("No jobs found");

    //println!("job_name:{}", selected_job_name);

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
