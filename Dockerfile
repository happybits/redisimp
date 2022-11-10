#
# USAGE:
# docker build -t redisimp ./
# docker run --entrypoint redisimp redisimp -s ip1:6379 -d ip2:6379


FROM python AS build
RUN pip install --upgrade pip


COPY ./ ./

RUN pip install -r dev-requirements.txt

RUN python3 setup.py bdist_wheel


FROM python

RUN pip install --upgrade pip

COPY --from=build ./dist/redisimp-*-py3-none-any.whl ./

RUN pip install ./redisimp-*-py3-none-any.whl

RUN rm ./redisimp-*-py3-none-any.whl

ENTRYPOINT ["redisimp"]