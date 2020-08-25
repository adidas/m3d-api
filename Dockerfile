FROM amazonlinux:2.0.20190508

# Installing reuqired packages
RUN yum update -y && \
    yum install -y \
        zip \
        unzip \
        sendmail \
        mailx \
        tar \
        bzip2 \
        gzip \
        wget \
        git \
        which \
        gcc-c++ \
        python3 \
        python3-devel

# Installing Python dependencies
COPY requirements.txt /tmp/m3d-api-requirements.txt
RUN pip3 install -r /tmp/m3d-api-requirements.txt --ignore-installed chardet && \
    pip3 install awscli==1.16.96 && \
    rm /tmp/m3d-api-requirements.txt

RUN groupadd -r m3d && \
    useradd -r -g m3d m3d && \
    mkdir -p /home/m3d && \
    chown m3d:m3d /home/m3d
USER m3d

CMD ["/bin/bash"]
