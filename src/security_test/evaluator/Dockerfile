FROM eclipse-temurin:11-jdk

# install gradle
RUN apt-get update && apt-get install -y unzip && rm -rf /var/lib/apt/lists/*
RUN wget -q -P /tmp https://services.gradle.org/distributions/gradle-7.1-bin.zip
RUN unzip -q -d /opt/gradle /tmp/gradle-7.1-bin.zip
RUN chmod +x /opt/gradle/gradle-7.1/bin/gradle
RUN ln -s /opt/gradle/gradle-7.1/bin/gradle /usr/bin/gradle

WORKDIR /build
COPY build.gradle.kts /build/build.gradle.kts
COPY settings.gradle.kts /build/settings.gradle.kts
RUN gradle dependencies

COPY src/ /build/src
RUN gradle installDist
RUN mkdir /app
RUN cp -r ./build/install/emailtest.evaluator/* /app/

WORKDIR /app
CMD ["./bin/emailtest.evaluator"]
