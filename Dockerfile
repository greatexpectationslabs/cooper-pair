FROM superconductive/substrate

RUN mkdir -p /var/pair

WORKDIR /var/pair

COPY dev-requirements.txt ./
COPY requirements.txt ./
COPY . ./

RUN pip3 install -r dev-requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install -e .

CMD py.test
