FROM hysds/pge-base:develop-es1

MAINTAINER jlinick "alexander.p.torres@jpl.nasa.gov"
LABEL description="USGS NEIC event evaluation"

USER ops
#RUN sudo yum install python3-pip -y

#RUN sudo yum install python34-pip -y \
#    && sudo yum clean all -y

#RUN source /opt/conda/bin/activate root \
#    && conda create -n gdal_env gdal
#ENV PATH /home/ops/.conda/envs/gdal_env/bin/:$PATH


# copy package
COPY . /home/ops/verdi/ops/usgs_neic_evaluator
RUN set -ex \
    && source /home/ops/verdi/bin/activate \
    && sudo chown -R ops:ops /home/ops/verdi/ops/usgs_neic_evaluator
    #&& sudo chown -R ops:ops /opt/conda/lib/python3.7/site-packages


# install water mask
RUN git clone https://github.com/hysds/lightweight_water_mask.git /home/ops/verdi/ops/lightweight_water_mask
RUN set -ex \
    &&sudo chown -R ops:ops /home/ops/verdi/ops/lightweight_water_mask \
    && cd /home/ops/verdi/ops/lightweight_water_mask \
    && /home/ops/verdi/bin/pip install -e .
    && pip install geojson


#RUN echo "source /opt/conda/bin/activate root" >> ~/.bashrc \
#    && echo "conda activate gdal_env" >> ~/.bashrc

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
