FROM eclipse-temurin:11-jdk

ENV OPENSSL_CONF=/etc/ssl/

RUN apt-get update
RUN apt-get install -y bsdmainutils dnsutils locales unzip

# install golang
RUN curl -O https://dl.google.com/go/go1.21.0.linux-amd64.tar.gz
RUN tar -xvf go1.21.0.linux-amd64.tar.gz
RUN mv go /usr/local
ENV GOROOT=/usr/local/go
ENV PATH=$PATH:$GOROOT/bin

# install gradle
RUN wget -q -P /tmp https://services.gradle.org/distributions/gradle-7.1-bin.zip
RUN unzip -q -d /opt/gradle /tmp/gradle-7.1-bin.zip
RUN chmod +x /opt/gradle/gradle-7.1/bin/gradle
RUN ln -s /opt/gradle/gradle-7.1/bin/gradle /usr/bin/gradle

# build gotls
WORKDIR /app/gotls
RUN go mod init module
RUN go get github.com/miekg/dns
RUN go build github.com/miekg/dns

RUN go get github.com/shuque/dane
RUN go build github.com/shuque/dane

RUN go get github.com/shuque/gotls
RUN go build github.com/shuque/gotls

RUN localedef -i en_US -f UTF-8 en_US.UTF-8

# install testssl
WORKDIR /app
COPY testssl.sh/ testssl/
COPY testssl.sh/bin/openssl.Linux.x86_64 /usr/bin/openssl
RUN chmod +x /usr/bin/openssl
RUN chmod +x /app/testssl/testssl.sh

COPY dss /usr/bin/dss
RUN chmod +x /usr/bin/dss

WORKDIR /build
COPY build.gradle.kts /build/build.gradle.kts
COPY settings.gradle.kts /build/settings.gradle.kts
RUN gradle dependencies

COPY src/ /build/src
RUN gradle installDist
RUN cp -r ./build/install/scanner/* /app/

WORKDIR /app
CMD ["./bin/scanner"]
