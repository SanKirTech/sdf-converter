#! /bin/bash
apt update
apt -y install python3 python3-pip git
git clone https://github.com/SanKirTech/sdf-converter.git
python3 -m pip install -r sdf-converter/requirements.txt