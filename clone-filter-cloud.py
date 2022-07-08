import hashlib
import itertools
import json
import logging
import os
import pathlib
import platform
import shutil
import stat
import subprocess
import sys
import traceback
import zipfile
from collections import namedtuple
from multiprocessing import cpu_count
from random import Random

import requests
from joblib import Parallel, delayed

mirrors = [
    "https://hub.fastgit.xyz",
    "https://hub.nuaa.cf",
    "https://hub.gitslow.tk",
    "https://hub.verge.tk",
    "https://hub.xn--gzu630h.xn--kpry57d",
    "https://hub.xn--p8jhe.tw",
]


def random_iter(list):
    size = len(list)
    index = Random().randint(a=0, b=size - 1)
    i = 0
    while i < size:
        idx = (index + i) % size
        yield idx, list[idx]
        i += 1


def clone(repo, repodir):
    for i, m in random_iter(mirrors):
        try:
            dir_tmp = f"{repodir}/{i}"
            
            ret = subprocess.run(
                f'git clone --depth 1 --config "http.sslverify=false" {m}/{repo} {dir_tmp}',
                capture_output=True,
                encoding="utf-8",
                timeout=1800,
            )
            
            dir_git = f"{dir_tmp}/.git"
            if os.path.isdir(dir_git):
                shutil.rmtree(dir_git, onerror=readonly_handler)
            if not ret.returncode:
                return True
        except:
            pass

    dir_tmp = f'{repodir}/.git'
    if os.path.isdir(dir_tmp):
        shutil.rmtree(dir_tmp, onerror=readonly_handler)

    os.system(f"echo {repo} >> fail.log")
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
            
            # 过滤py文件 > 重命名size-md5.py > 移到output-filter目录
            for pyfile in findpy(repodir):
                filter(pyfile)

        except Exception as e:
            print(f"repoerr : {line} -> {e}")

    if index % 100 == 0:
        print(f"done index:{index}")


# clone repos to output, filter to output-filtered
if __name__ == "__main__":
    Args = namedtuple(
        "Args",
        ["repos", "source", "filtered", "balance", "njobs", "debug"],
    )
    args = Args(
        "repos-reverse",
        "output-source",
        "output-filtered",
        0,
        cpu_count() + 1,
        False,
    )

    pathlib.Path(args.source).mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.filtered).mkdir(parents=True, exist_ok=True)

    with open(args.repos) as f:
        repos = f.readlines()
        repo_done = os.listdir(args.source)

        Parallel(n_jobs=args.njobs, prefer="processes", batch_size=10)(
            delayed(download_repo)(index, line) for index, line in enumerate(repos)
        )
