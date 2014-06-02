# --- !Ups

CREATE TABLE zookeepers (
  id VARCHAR,
  host VARCHAR,
  port INT,
  cluster VARCHAR,
  statusId LONG,
  groupId LONG,
  chroot VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE groups (
  id LONG,
  name VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE status (
  id LONG,
  name VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE consumptionData (
  id LONG NOT NULL AUTO_INCREMENT,
  factor LONG,
  timestmp LONG,
  name VARCHAR,
  cluster VARCHAR,
  topic VARCHAR,
  offsets LONG,
  PRIMARY KEY (id)
);

INSERT INTO groups (id, name) VALUES (0, 'ALL');
INSERT INTO groups (id, name) VALUES (1, 'DEVELOPMENT');
INSERT INTO groups (id, name) VALUES (2, 'PRODUCTION');
INSERT INTO groups (id, name) VALUES (3, 'STAGING');
INSERT INTO groups (id, name) VALUES (4, 'TEST');

INSERT INTO status (id, name) VALUES (0, 'CONNECTING');
INSERT INTO status (id, name) VALUES (1, 'CONNECTED');
INSERT INTO status (id, name) VALUES (2, 'DISCONNECTED');
INSERT INTO status (id, name) VALUES (3, 'DELETED');

# --- !Downs

DROP TABLE IF EXISTS zookeepers;
DROP TABLE IF EXISTS groups;
DROP TABLE IF EXISTS status;
DROP TABLE IF EXISTS consumptionData;