# Setup Instructions for A3
## Environment Setup

This is the setup guide for the A3 environment, running on WSL
You could use pyenv, conda, or any other environment manager of your choice. I am using conda.

```bash
conda create -n dic_a3 python=3.11.6 -c conda-forge
conda activate dic_a3
```
You can confirm the python version with:

```bash
python --version # Should now show python 3.11.6
```
## Install required packages 


```bash
sudo apt update
sudo apt install jq zip curl
```

## Install docker
If you don't have Docker installed, you can install it:
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
```


## Install Dependencies
```bash
pip install -r requirements.txt
```

## Start LocalStack
Now you should be able to start LocalStack with the following command:
```bash
LOCALSTACK_ACTIVATE_PRO=0 LOCALSTACK_DEBUG=1 localstack start
```

This will start LocalStack in debug mode in you terminal, keep the terminal open.

you can open a second terminal and run the following command to check if LocalStack is running:
```bash
conda activate dic_a3 # dont forget to activate the conda environment
curl http://localhost:4566/_localstack/health
```

## Setup Script

The setup script will create all the buckets, lambda functions etc. as described in the tutorial in a automated way.

```bash
chmod +x setup.sh
./setup.sh
```
