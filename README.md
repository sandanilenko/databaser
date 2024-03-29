# Databaser

Инструмент для работы с Postgres, позволяющий получать срез базы данных согласно указанным записям ключевой таблицы.

## Основные сущности

- База данных - сущность, имитирующая реальную базу данных;
- Таблица - таблица базы данных;
- Колонка - колонка таблицы;
- Подключение к базе данных - хранит в себе данные для создания пула соединений;
- Запрос - SQL-код, который должен быть исполнен на выбранной БД;
- Исполнитель запросов - сущность для исполнения запросов на выбранной БД;
- Сборщик данных - осуществляет сборку идентификаторов записей из базы-донора;
- Переносщик данных - выполняет формирование запросов на перенос данных из базы донора в целевую базу и инициирует исполнение.

## Описание концепции

В большинстве проектов есть бизнес-сущность, вокруг которой построена вся логика работы. Это может быть учреждение, 
компания, пользователь и т.п. Бизнес-сущность имеет отражение как в программной реализации, так и в виде сущности в 
базе данных. С ростом базы данных, если есть необходимость работы с реальными данными, увеличивается как время на 
восстановление актуального дампа на локальной машине разработчика, так и объем занимаемой памяти. 

Данный инструмент помогает получить срез базы данных, в виде новой базы данных со схемой, идентичной исходной базы 
данных (донора) или сокращенной, согласно определенным проектом правилам. Собранных данных должно хватать для решения 
задач или для запуска отдельного инстанса приложения для специфического клиента.

## Принцип работы

Перед началом работы переносчика данных необходимо, чтобы база-донор и целевая база находились в рабочем состоянии и их 
структуры были согласованными.

На время работы, в целевой базе отключаются все ограничения. Это сделано для исключения ошибок с неверным порядком 
загружаемых записей. Часть записей может быть выгружена ранее, чем те, на которые производится ссылка. Это связано с 
циклическими зависимостями между таблицами и записями таблиц.

Помимо отключенных ограничений, на время работы Databaser, отключаются все триггеры. Это сделано с целью отключения 
всех проверок на время переноса данных, т.к. в хранимых процедурах могут быть те, которые занимаются дополнительной 
проверкой целостности данных, исходя из определенных бизнес-требований.

Когда эти условия выполнены Databaser может начинать свою работу.

Схематическое представление компонентов, учавствующих в процессе сбора и переноса данных

![Схема работы Databaser][scheme_of_work]

Отредактировать схему можно на странице в [Confluence](https://conf.bars.group/pages/viewpage.action?pageId=62541042). 
Также ее необходимо обновить в репозитории.

Создаются две базы данных с параметрами подключения к целевой базе данных и базе-донору. После чего создается пул 
соединений к целевой базе и выполняется создание структуры базы данных целевой БД в виде Python-объектов в памяти. В 
это время создаются таблицы и колонки. В процессе построения структуры базы данных вычисляются внешние ссылки на 
таблицы, как прямые, так и обратные.

После того как структура целевой базы данных построена, можно приступать к дальнейшим действиям. Создается пул 
подключений к базе-донору. 

Далее будет производиться выборка идентификаторов записей, которые должны быть перенесены. 
Первым делом вычисляются идентификаторы всей иерархии учреждений для указанных в настройках. Все полученные 
идентификаторы записываются в таблицы созданной структуры базы. После получения идентификаторов учреждений 
производится выбрка записей всех таблиц, которые имеют внешние ключи на таблицу учреждений. Внешние ссылки необязательно 
могут быть ключами, но, например, и полями хранящими целочисленные значения. Если у таблицы есть внешний 
ключ на таблицу учреждений, то остальные внешние ключи не рассматриваются. После того как была произведена выборка 
всех необходимых записей таблиц ссылающихся на учреждение, производится выборка записей оставшихся таблиц. Остальные 
таблицы сортируются по количеству внешних ключей на таблицы, данные которых уже были получены из базы-донора. 

После выбора остальных записей создается новый пул подключений к целевой БД для выполнения запросов при помощи 
FDW-расширения PostgreSQL. В процессе формируются запросы на получение всех данных с указанными идентификаторами 
записей таблиц. Таким образом происходит перенос данных из базы-донора в целевую базу данных.

### Диаграмма классов

![Диаграмма классов Databaser][databaser_class_diagram]

Отредактировать схему можно на странице в [Confluence](https://conf.bars.group/pages/viewpage.action?pageId=62541042).
Также ее необходимо обновить в репозитории.

## Требования к окружению

Для работы понадобится установленный Docker-engine на локальной машине, согласно инструкции 
[Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

## Инструкция по применению

Перед запуском Databaser, необходимо поднять контейнер с Postgres и целевой базой данных, согласованной с базой данных 
донором. В качестве базового образа можно использовать официальный образ Postgres с 
[Dockerhub](https://hub.docker.com/_/postgres), в который будет необходимо раскатить миграции продукта, для получения 
согласованной схемы.

Далее запускается контейнер с Databaser, в который пробрасывается конфигурационный файл, в виде .env-файла или 
указываются переменные окружения с параметрами запуска Databaser. Список параметров конфигурационных файлов описан ниже.

### Параметры конфигурационного файла

- DATABASER_LOG_LEVEL - Уроверь логирования. Допустимые значения: NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL;
- DATABASER_TEST_MODE - При установленном значении True, asyncio будет работать в отладочном режиме. Допустимые значения: True, False;
- DATABASER_SRC_DB_HOST - IP-адрес или наименование сервиса с базой донором;
- DATABASER_SRC_DB_PORT - Порт для доступа к базе донору;
- DATABASER_SRC_DB_NAME - Имя базы данных донора;
- DATABASER_SRC_DB_USER - Пользователь базы данных донора;
- DATABASER_SRC_DB_PASSWORD - Пароль пользователя базы данных донора;
- DATABASER_DST_DB_HOST - IP-адрес машины или имя сервиса с целевой базой данных;
- DATABASER_DST_DB_PORT - Порт для доступа к целевой базе данных;
- DATABASER_DST_DB_NAME - Имя целевой базы данных;
- DATABASER_DST_DB_USER - Пользователь для доступа к целевой базе данных;
- DATABASER_DST_DB_PASSWORD - Пароль пользователя для доступа к целевой базе данных;
- DATABASER_KEY_TABLE_NAME - Имя ключевой таблицы, которая будет в основе среза данных;
- DATABASER_KEY_COLUMN_NAMES - Имя ключевого поля. Если наименований несколько, то они должны быть перечислены через запятую;
- DATABASER_KEY_COLUMN_VALUES - Идентификаторы записей ключевой таблицы. Если идентификаторов несколько, то они перечисляются через запятую;
- DATABASER_KEY_TABLE_HIERARCHY_COLUMN_NAME - Имя колонки ключевой таблицы служащей для построения иерархию;
- DATABASER_EXCLUDED_TABLES - Перечисление таблиц через запятую, без пробелов, исключаемых для переноса данных в целевую БД;
- DATABASER_TABLES_LIMIT_PER_TRANSACTION - Ограничение по количеству обрабатываемых таблиц в одной транзакции. Например, при очистке таблиц;
- DATABASER_TABLES_WITH_GENERIC_FOREIGN_KEY - Таблицы с Generic Foreign Key, актуально для проектов, основанных на Django;
- DATABASER_IS_TRUNCATE_TABLES - Необходимо зачищать таблицы перед переносом данных. Допустимые значения: True, False;
- DATABASER_TABLES_TRUNCATE_INCLUDED - Таблицы предназначенные для зачистки перед переносом данных;
- DATABASER_TABLES_TRUNCATE_EXCLUDED - Таблицы исключаемые от зачистки перед переносом данных.

Все параметры конфигурационного файла можно поместить в .env-файл и при запуске передать в контейнер, при помощи 
параметра --env-file. Или передать каждый параметр отдельно, при помощи ключа -e.

## Сборка и распространение

### Сборка
Databaser распространяется в виде Docker-образа, что снижает количество зависимостей, необходимых для установки на 
локальной машине. Для надежности и возможности организации открытого доступа, образ выкладывается как на внутренний 
[Nexus](http://nexus.budg.bars.group/), так и в [Docker Hub](https://hub.docker.com/).

```bash
$ git pull origin master

# Для Nexus
$ IMAGE_NAME=nexus.budg.bars.group/databaser:latest

# Для Docker Hub
$ IMAGE_NAME=sandanilenko/databaser:latest

$ docker build --tag $IMAGE_NAME .
$ docker push $IMAGE_NAME
```

### Скачивание

Для скачивания образа на локальную машину необходимо выполнить команду:

```bash
# Для выкачивания из Nexus
$ docker pull nexus.budg.bars.group/databaser:latest

# Для выкачивания из Docker Hub
$ docker pull sandanilenko/databaser:latest
```

На основе полученного образа необходимо запустить контейнер.

## Инструкция для разработчика

### Клонирование проекта

Перед началом внесения правок, необходимо склонировать проект Databaser на локальную машину. Проект находится на 
[GitLab](ssh://git@192.168.233.37:8022/a.danilenko/databaser.git) или 
[GitHub](https://github.com/sandanilenko/databaser) и является публичным.  Для внесения правок, 
необходимо создать форк проекта и работать с ним.

Предполагается, что разработчик работает в Unix-подобной системе и в корневой директории пользователя имеется 
директория projects.

```bash
$ cd ~/projects/
```

Клонирование проекта выполняется с помощью команды

```bash
$ git clone ssh://git@192.168.233.37:8022/<username>/databaser.git

или

$ git clone git@github.com:<username>/databaser.git
```

### База-донор

Существует два случая, где может находиться база-донор:

1. В качестве базы-донора выступает база тестогово стенда;
2. В качестве базы-донора выступает локально поднятая база.

Для работы с базой-донором, необходимо будет внести правки в конфигурационный файл Databaser в части параметров 
src_db_*.

### Настройка PyCharm для работы

После того как создана локальная тестовая целевая база и сгенерирован конфигурационный файл в директории проекта 
Databaser, можно приступить к конфигурированию PyCharm для запуска из него сборок.

Существует два варианта запуска Databaser - локально, как Python приложение и в Docker-контейнере. Будут рассмотрены 
оба варианта.

#### Запуск Python приложения

Для работы требуется Python 3.9. Поэтому нужно будет воспользоваться pyenv, для более быстрой 
установки свежей версии Python на локальной машине. Pyenv можно установить в систему согласно инструкции от
[realpython.com](https://realpython.com/intro-to-pyenv/). После установки pyenv, нужно следовать инструкции ниже.

Первоначально необходимо создать виртуальное окружение, в которое будут установлены зависимости проекта. Для этого 
необходимо выполнить команды:

```bash
$ pyenv install -v 3.9.5
# Если возникла ошибка с компиляцией ssl, то необходимо указать директорию с ssl
$ CONFIGURE_OPTS="--with-openssl=<SSL_DIR_PATH>" pyenv install -v 3.9.5
$ pyenv virtualenv 3.9.5 databaser
$ pyenv activate databaser
$ pip install -r requirements.txt -r requirements-dev.txt
```

Для работы в PyCharm необходимо создать следующую конфигурацию:

![Конфигурация запуска приложения в PyCharm][python-app-1]

В переменных окружения необходимо указать все параметры конфигурирования. После того как конфигурирование завершено, 
можно запускать сборку.

#### Запуск в контейнере

При данном подходе, все переменные окружения - параметры запуска, упаковываются в bodatabaser_build.env, находящийся в 
корне проекта.

Для запуска в контейнере необходимо создать конфиг Dockerfile:

![Docker][docker-1]

В качестве сервера нужно создать новый сервер Docker и использовать Unix socket. Если сервер был настроен ранее, то 
нужно использовать его:

![Docker][docker-2]

Далее необходимо произвести настройку:

![Docker][docker-3]

| Параметр        | Значение           |
| ------------- |:-------------:|
| Dockerfile      | Dockerfile |
| Context folder      | .      |
| Build options | --no-cache      |
| Command | /bin/bash -c "set -o allexport && source /tmp/databaser/bodatabaser_build.env && set +o allexport && python3 /srv/databaser/manage.py"      |
| Buind mounts | /home/<username>/projects/databaser:/tmp/databaser:ro      |
| Run options | --privileged --name=databaser --memory=8g --shm-size=4g --network=bonet      |

Перед запуском, необходимо убедиться, что значение параметра DATABASER_DST_DB_HOST соответствует наименованию 
контейнера с тестовой целевой базой. Здесь используются имена сервисов, т.к. контейнеры запускаются в одной сети. 
Ничего не мешает использовать ip-адрес машиный вместо имени сервиса.

После того как конфигурирование завершено, можно запускать сборку.

## Внесение изменений, оповещение о создании ПР

Все изменения в кодовую базу делаются через ПР (Pull Request). Ранее уже обозначалось, что работы нужно производить с 
форком проекта Databaser в отдельной созданной ветке. После окончания работ по внесению изменений, необходимо создать 
ПР в основной проект.

После того как ПР будет создан, необходимо оповестить разработчика о наличии ПР и необходимости его смержить, если ПР 
подвисает на несколько дней.

[scheme_of_work]: ./images/scheme_of_work.png "Scheme of work"
[databaser_class_diagram]: ./images/databaser_class_diagram.png "Databaser class diagram"
[python-app-1]: ./images/python-app-1.png "Python Application"
[docker-1]: ./images/docker-1.png "Docker 1"
[docker-2]: ./images/docker-2.png "Docker 2"
[docker-3]: ./images/docker-3.png "Docker 3"