FROM scratch

ADD bin/bqshift /bqshift

ENTRYPOINT ["/bqshift"]