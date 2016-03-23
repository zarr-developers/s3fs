## Building conda packages

A Conda `noarch` package for s3fs can be built on linux-64 (tested on Ubuntu
14.04) using the following commands:

```
export CONDA_DIR=~/miniconda2

sudo apt-get update
sudo apt-get install git -y

curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o ~/miniconda.sh
bash ~/miniconda.sh -b -p $CONDA_DIR
$CONDA_DIR/bin/conda install conda-build anaconda-client -y

git clone https://github.com/dask/s3fs.git ~/s3fs
cd ~/s3fs
$CONDA_DIR/bin/conda build conda.recipe

$CONDA_DIR/bin/anaconda login
$CONDA_DIR/bin/anaconda upload $CONDA_DIR/conda-bld/*/*.tar.bz2 -u dask
```
