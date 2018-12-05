FROM superconductive/substrate

RUN mkdir -p /var/pair

WORKDIR /var/pair

COPY test_requirements.txt ./

RUN pip3 install -r test_requirements.txt

COPY requirements.txt ./

RUN pip3 install -r requirements.txt

COPY . ./

RUN pip3 install -e .

CMD py.test
