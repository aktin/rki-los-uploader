FROM python:3.9

WORKDIR /opt

RUN python -m pip install --upgrade pip
RUN python -m pip install pytest

# Install R 4.1.2
RUN apt update
RUN apt install --no-install-recommends software-properties-common dirmngr -y
RUN apt install libssl-dev libcurl4-openssl-dev unixodbc-dev libxml2-dev libmariadb-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev -y
RUN wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc
RUN echo | add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
RUN apt install --no-install-recommends r-base -y

ADD scripts/* ./
ADD resources/* ./
ADD keys/* ./

RUN chmod +x count_tag_in_status_xml.sh
