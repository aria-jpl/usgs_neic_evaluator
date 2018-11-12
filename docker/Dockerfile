FROM hysds/pge-gdal:latest

MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="USGS NEIC event evaluation"

ARG GIT_OAUTH_TOKEN

USER ops

# copy package
COPY . /home/ops/verdi/ops/usgs_neic_evaluator
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/usgs_neic_evaluator \
 && sudo chown -R ops:ops /usr/lib/python2.7/site-packages


# install water mask
RUN git clone https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/aria-hysds/lightweight_water_mask.git /home/ops/verdi/ops/lightweight_water_mask
RUN sudo chown -R ops:ops /home/ops/verdi/ops/lightweight_water_mask
RUN set -ex \
  && cd /home/ops/verdi/ops/lightweight_water_mask \
  && python setup.py install

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
