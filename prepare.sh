#!/bin/bash

openssl req -new -days 365 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:*.localhost,DNS:s3.lan,DNS:*.s3.lan" \
  -nodes -x509 -newkey ec \
  -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout root.key -out root.crt

# On some kernels the kTLS module must be loaded first as follows.
sudo modprobe tls

python3 -m pip install --upgrade --user pip virtualenv

pip3 install awscli --upgrade --user

# install virtual environment and install dependencies in it
VENV_NAME='.tests-venv'
python3 -m venv $VENV_NAME

.tests-venv/bin/pip3 install -r requirements.txt

text_reset=$(tput sgr0)  # Text reset
text_green=$(tput setaf 2)  # Test green

echo "
Virtual environment '$VENV_NAME' for Python has been created.
run $text_green 'source $VENV_NAME/bin/activate' $text_reset to activate environment.
run $text_green 'deactivate' $text_reset to deactivate environment.
"
