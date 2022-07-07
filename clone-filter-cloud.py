import hashlib
import itertools
import json
import logging
import os
import platform
import shutil
import stat
import subprocess
import sys
import traceback
from collections import namedtuple
from random import Random
import zipfile
import pathlib


import requests
from joblib import Parallel, delayed
from multiprocessing import cpu_count


def clone(user, repo):
    dir = f"{args.source}/{user}-{repo}"
    try:
        ret = subprocess.run(
            f'git clone --depth 1 --config "http.sslverify=false" https://1:1@github.com/{user}/{repo} {dir}',
            capture_output=True,
            encoding="utf-8",
            timeout=1800,
        )
        if ret.returncode:
            os.system(f"echo {user} {repo} {ret.stderr.encode()} >> fail.log")
        else:
            return True
    except Exception as e:
        os.system(f"echo {user} {repo} {e} >> fail.log")
    return False


def filter(pyfile):
    try:
        if not pyfile.endswith(".py") or not os.path.isfile(pyfile):
            os.remove(pyfile)
            return

        size = os.path.getsize(pyfile)
        if not 0 < size < 1048577:
            return

        myhash = hashlib.md5()
        with open(pyfile, "rb") as f:
            for line in f:
                if len(line.decode("utf-8")) > 1000:
                    return
                myhash.update(line)
        md5 = myhash.hexdigest()
        balance_dir = md5[0 : args.balance]

        pyfile_new = f"{args.filtered}/{balance_dir}/{size}-{md5}.py"
        shutil.move(pyfile, pyfile_new)

    except Exception as e:
        if args.debug:
            print(f"pyerr : {pyfile} -> {e}")


def findpy(base):
    for root, ds, fs in os.walk(base):
        for f in fs:
            yield os.path.join(root, f)


def readonly_handler(func, path, execinfo):
    try:
        if os.path.exists(path):
            os.chmod(path, stat.S_IWRITE)
            func(path)
    except:
        pass


def download_repo(index, line):
    repo_url = line.strip().split(",")[0]
    repo_split = repo_url.split("/")
    user = repo_split[0]
    repo = repo_split[1]
    repo_uniq = f"{user}-{repo}"
    repodir = f"{args.source}/{repo_uniq}"
    if repo_uniq not in repo_done:
        try:
            # 下载repo
            success = clone(user, repo)
            if success:
                # 删除.git文件夹
                gitdir = f"{repodir}/.git"
                shutil.rmtree(gitdir, onerror=readonly_handler)

                # 过滤py文件 > 重命名size-md5.py > 移到output-filter目录
                for pyfile in findpy(repodir):
                    filter(pyfile)

        except Exception as e:
            print(f"repoerr : {line} -> {e}")
            # print(traceback.format_exc())

    if index % 100 == 0:
        print(f"done index:{index}")


# clone repos to output, filter to output-filtered
if __name__ == "__main__":
    Args = namedtuple(
        "Args",
        ["repos", "source", "filtered", "log", "balance", "clean", "njobs", "debug"],
    )
    args = Args(
        "repos-reverse",
        "output-source",
        "output-filtered",
        "log",
        0,
        False,
        cpu_count() + 1,
        False,
    )

    pathlib.Path(args.source).mkdir(parents=True, exist_ok=True) 
    pathlib.Path(args.filtered).mkdir(parents=True, exist_ok=True) 

    with open(args.repos) as f:
        repos = f.readlines()[:10]
        repo_done = os.listdir(args.source)

        Parallel(n_jobs=args.njobs, prefer='processes', batch_size=10)(
            delayed(print)(index, line) for index, line in enumerate(repos)
        )
