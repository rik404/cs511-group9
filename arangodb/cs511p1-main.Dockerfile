####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM cs511p1-common

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

COPY spark-resources/main $SPARK_HOME/conf/

COPY ./setup-main.sh ./setup-main.sh
RUN /bin/bash setup-main.sh

COPY ./start-main.sh ./start-main.sh
CMD ["/bin/bash", "start-main.sh"]

ENV PYTHONPATH $SPARK_HOME/python/:$PYTHONPATH

COPY ./code ./code
# COPY ./code/sparkLoader.py ./sparkLoader.py

# ENTRYPOINT [ "bash", "./code/docker-entrypoint.sh" ]
# CMD ["spark-submit", "arangoLoader.py"]
