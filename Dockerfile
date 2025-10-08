FROM xemuliam/dbt:1.9-snowflake

WORKDIR /Robling

ENV EMAIL_FROM="ops@robling.io"
ENV EMAIL_FROM_NAME="Do-Not-Reply"

COPY . .

RUN pip --no-cache-dir install -r ./requirements.txt

ENTRYPOINT ["python3", "dbt_runner.py"]