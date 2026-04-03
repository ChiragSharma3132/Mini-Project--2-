FROM eclipse-temurin:17

WORKDIR /app

COPY . .

# lib folder ko include karo
RUN javac -cp ".:lib/*" backend/dbengine/*.java

EXPOSE 3000

CMD ["java", "-cp", ".:lib/*:backend/dbengine", "dbengine.MainServer"]