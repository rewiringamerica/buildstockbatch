ARG OS_VER
FROM --platform=linux/amd64 nrel/openstudio:$OS_VER as buildstockbatch
ARG CLOUD_PLATFORM=aws
ENV DEBIAN_FRONTEND=noninteractive
COPY . /buildstock-batch/

RUN apt update && apt install -y wget
RUN mkdir -p ~/miniconda3
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
RUN bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
RUN . ~/miniconda3/bin/activate
ENV PATH="/root/miniconda3/bin:$PATH"
RUN echo "PATH=$PATH"
RUN conda init --all
RUN conda create -n bsb311 python=3.11
RUN conda init bash
RUN echo "conda activate bsb311" >> ~/.bashrc
ENV PATH="/root/miniconda3/envs/bsb311/bin:$PATH"
RUN echo "PATH=$PATH"

RUN python -m pip install "/buildstock-batch[${CLOUD_PLATFORM}]"

# Base plus custom gems
FROM buildstockbatch as buildstockbatch-custom-gems
RUN sudo cp /buildstock-batch/Gemfile /var/oscli/
# OpenStudio's docker image sets ENV BUNDLE_WITHOUT=native_ext
# https://github.com/NREL/docker-openstudio/blob/3.2.1/Dockerfile#L12
# which overrides anything set via bundle config commands.
# Unset this so that bundle config commands work properly.
RUN unset BUNDLE_WITHOUT
# Note the addition of 'set' in bundle config commands
RUN bundle config set git.allow_insecure true
RUN bundle config set path /var/oscli/gems/
RUN bundle config set without 'test development native_ext'
RUN bundle install --gemfile /var/oscli/Gemfile
