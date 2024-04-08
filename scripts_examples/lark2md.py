### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)


import os
### utils
def os_system_sure(command):
    print(f"执行命令：{command}")
    result = os.system(command)
    if result != 0:
        print(f"命令执行失败：{command}")
        exit(1)
    print(f"命令执行成功：{command}")




import sys
os_system_sure("wget -O _lark2md.py https://raw.githubusercontent.com/ActivePeter/paTools/main/lark2md.py")
os_system_sure(f"python3 _lark2md.py {sys.argv[1]} {sys.argv[2]} Q3c6dJG5Go3ov6xXofZcGp43nfb")
os_system_sure("rm -f _lark2md.py")
