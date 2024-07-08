import os

def batch_rename(directory):
    for filename in os.listdir(directory):
        
        os.rename(
            os.path.join(directory, filename),
            os.path.join(directory, filename.replace("cpu.mt","cpu.nml0.mt"))
        )
    print("Renaming completed.")

# 示例用法
directory = "records"
batch_rename(directory)
