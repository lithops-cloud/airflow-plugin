SEN2COR_EXEC=$(echo $SEN2COR_URL | rev | cut -d / -f1 | rev)

# Build and install python2.7 as requirement for Sentinel
apt update -y && apt install --no-install-recommends -y \
    wget \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    python3-wheel
apt clean && rm -rf /var/lib/apt/lists/*

# Install Sentinel software
wget $SEN2COR_URL -q
chmod u+x $SEN2COR_EXEC
/bin/bash $SEN2COR_EXEC
rm -rf $SEN2COR_EXEC
