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
        python3-devel \
        unixODBC-devel

# Installing Oracle client
RUN wget -q -O /etc/yum.repos.d/public-yum-ol7.repo http://yum.oracle.com/public-yum-ol7.repo && \
    wget -q -O /tmp/RPM-GPG-KEY-oracle-ol7 http://yum.oracle.com/RPM-GPG-KEY-oracle-ol7 && \
    rpm --import /tmp/RPM-GPG-KEY-oracle-ol7 && \
    rm /tmp/RPM-GPG-KEY-oracle-ol7 && \
    yum install -y yum-utils && \
    yum-config-manager --enable ol7_oracle_instantclient && \
    yum -y install oracle-instantclient18.3-basiclite && \
    yum clean all

# Installing Python dependencies
COPY requirements.txt /tmp/m3d-api-requirements.txt
RUN pip3 install -r /tmp/m3d-api-requirements.txt --ignore-installed chardet && \
    pip3 install awscli==1.16.96 && \
    rm /tmp/m3d-api-requirements.txt

# Setting environment variables
ENV LD_LIBRARY_PATH=/usr/lib/oracle/18.3/client64/lib:$LD_LIBRARY_PATH

CMD ["/bin/bash"]
