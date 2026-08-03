FROM docker.io/adfreiburg/qlever-ui@sha256:2bdbfac438c8d43ae5337bb547993b1880e5c43b63871c74ca180aa123644851

# The upstream template includes a leading slash that is absent from Django's
# production static-files manifest. Keep DEBUG disabled and correct the lookup.
RUN sed -i \
    "s|static '/wasm/formatter/index.js'|static 'wasm/formatter/index.js'|" \
    /app/backend/templates/partials/head.html \
    && ! grep -q "static '/wasm/formatter/index.js'" \
    /app/backend/templates/partials/head.html

COPY bootstrap.py /opt/climatesense/bootstrap.py
