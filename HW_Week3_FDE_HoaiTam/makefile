include env

build:
	docker-compose build

up:
	docker-compose --env-file env up -d

down:
	docker-compose --env-file env down

restart:
	make down && make up

to_psql:
	docker exec -ti de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

to_mysql:
	docker exec -it de_mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root:
	docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}


FILES ?= olist_products_dataset.csv product_category_name_translation.csv olist_orders_dataset.csv olist_order_items_dataset.csv olist_order_payments_dataset.csv
CSV_DIR ?= /tmp/brazilian-ecommerce

load_to_mysql:
	@for file in $(FILES); do \
	  echo "Loading $$file..."; \
	  docker exec -it de_mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e "\
	    LOAD DATA LOCAL INFILE '$(CSV_DIR)/$$file' \
	    INTO TABLE $${file%.csv} \
	    FIELDS TERMINATED BY ',' \
	    LINES TERMINATED BY '\n' \
	    IGNORE 1 ROWS;"; \
	done; \
	echo "Done loading all files!"

