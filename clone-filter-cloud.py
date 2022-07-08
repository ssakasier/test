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

import requests
from joblib import Parallel, delayed

mirrors = [
    "https://hub.fastgit.xyz",
    "https://hub.nuaa.cf",
    "https://hub.gitslow.tk",
    "https://hub.verge.tk",
    "https://hub.xn--gzu630h.xn--kpry57d",
    "https://hub.xn--p8jhe.tw",
    # ' --config "http.proxy=socks5://127.0.0.1:7788" https://1:1@github.com',
    # ' --config "ssl.verify=false" https://1:1@github.com',
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
            # suc, res = runcmd_safe(
                # f"git clone --depth 1 https://123:123@github.com/{repo}.git {repodir}",
                # f"git clone --depth 1 https://www.github.com/{repo}.git {repodir}",
                # f"git clone --filter=blob:none --sparse --depth 1 {m}/{repo} {repodir}",
            #     f"git clone --depth 1 {m}/{repo} {dir_tmp}",
            #     perr=True,
            # )
            ret = subprocess.run(
                f'git clone --depth 1 --config "http.sslverify=false" {m}/{repo} {dir_tmp}',
                capture_output=True,
                encoding="utf-8",
                timeout=1800,
            )
            # if os.path.isdir(repodir):
            #     suc2, res2 = runcmd_safe(
            #         f"git -C {repodir} sparse-checkout set *.py", perr=True
            #     )
            #     if suc2:
            #         return
            dir_git = f"{dir_tmp}/.git"
            if os.path.isdir(dir_git):
                shutil.rmtree(dir_git, onerror=readonly_handler)
            if not ret.returncode:
                return
        except:
            pass
    os.system(f"echo {repo} >> fail.log")

    if os.path.isdir(repodir):
        shutil.rmtree(repodir, onerror=readonly_handler)
        
    raise Exception("no more mirror", repo)


def get_ball(user, repo):
    ball = f"{args.source}/{user}-{repo}.zip"
    dst_dir = f"{args.source}/{user}-{repo}"
    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)

    for m in random_iter(mirrors):
        try:
            r = requests.get(f"{m}/{user}/{repo}/archive/refs/heads/master.zip")
            with open(ball, "wb") as f:
                for chunk in r.iter_content(chunk_size=512):
                    f.write(chunk)

            iszip = zipfile.is_zipfile(ball)
            if iszip:
                fz = zipfile.ZipFile(ball, "r")
                for file in fz.namelist():
                    if file.endswith(".py"):
                        fz.extract(file, dst_dir)

            return
        except Exception as e:
            print(m, user, repo, e)

    raise Exception("no more mirror")


def runcmd_safe(command, pout=False, perr=True):
    try:
        ret = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            shell=True,
        )
    except Exception as e:
        print(e)

    if ret.returncode:
        if perr:
            print(ret.stdout)
        return False, None
    else:
        if pout:
            print(ret)
        return True, ret.stdout.strip()


def runcmd(command, check=False, pout=False):
    try:
        ret = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            shell=True,
        )
        if check and ret.returncode:
            raise Exception(ret)
        if pout or check:
            print(ret)
        return ret.stdout.strip()
    except Exception as e:
        print(ret)
        if check:
            raise e


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

        # print(f"{pyfile} -> {pyfile_new}")
    except Exception as e:
        print(f"pyerr : {pyfile} -> {traceback.format_exc()}")


def findpy(base):
    for root, ds, fs in os.walk(base):
        for f in fs:
            yield os.path.join(root, f)


def remove(path):
    if platform.system() == "Windows":
        runcmd(f'powershell rm -Force -Recurse "{path}"')
    else:
        runcmd(f'rm -rf "{path}"')


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
    if repo_uniq not in repo_done:
        try:
            # 下载repo
            repodir = f"{args.source}/{repo_uniq}"
            clone(repo_url, repodir)

            # 删除.git文件夹
            # gitdir = f"{repodir}/.git"
            # shutil.rmtree(gitdir, onerror=readonly_handler)

            # 过滤py文件 > 重命名size-md5.py > 移到output-filter目录
            for pyfile in findpy(repodir):
                filter(pyfile)

            # 将repo目录置空
            # if args.clean:
            #     remove(f"{repodir}/*")

            if os.path.isdir(repodir):
                os.mkdir(f"{repodir}/.done")

        except Exception:
            print(f"repoerr : {line} -> {traceback.format_exc()}")
            # print(traceback.format_exc())

    print(user, repo)
    if index % 100 == 0:
        print(f"done index:{index} ")


# clone repos to output, filter to output-filtered
if __name__ == "__main__":
    Args = namedtuple(
        "Args",
        ["repos", "source", "filtered", "log", "balance", "clean", "njobs", "batch"],
    )
    args = Args(
        "repos-after-48w",
        "../output/output-source2",
        "../output/output-filtered",
        "log",
        0,
        False,
        16,
        1,
    )

    Log_Format = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
    cs_handler = logging.StreamHandler()
    cs_handler.setLevel(logging.INFO)
    cs_handler.setFormatter(Log_Format)
    file_handler = logging.FileHandler("log.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(Log_Format)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(cs_handler)
    logger.addHandler(file_handler)

    if not os.path.isdir(args.source):
        os.makedirs(args.source)
    if not os.path.isdir(args.filtered):
        os.makedirs(args.filtered)
    if args.balance > 0:
        for i in itertools.product("0123456789abcdef", repeat=args.balance):
            balance_dir = f'{args.filtered}/{"".join(i)}'
            if not os.path.isdir(balance_dir):
                os.makedirs(balance_dir)

    repo_done = os.listdir(args.source)

    while True:

        res = requests.post("http://127.0.0.1:8080/repos/piece/generate")
        print(res)
        a = json.loads(res.text)
        piece_id = a["members"][0]
        repos = a["members"][1]
        print(f"get piece: {piece_id} counts: {len(repos)}")

        Parallel(n_jobs=args.njobs, prefer="processes", timeout=1800)(
            delayed(download_repo)(index, line) for index, line in enumerate(repos)
        )

        requests.post(f"http://127.0.0.1:8080/repos/piece/{piece_id}/2")
