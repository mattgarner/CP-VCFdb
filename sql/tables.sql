drop database VCFdb;
create database VCFdb;
use VCFdb;


CREATE TABLE plate (
  pid                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name                VARCHAR(20) NOT NULL ,

  KEY name_idx (name)
);



CREATE TABLE sample (

  sid                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name                VARCHAR(40) NOT NULL ,

  KEY name_idx (name)
);



CREATE TABLE sample_sequence (

  ssid                INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  sid                 INT NOT NULL,
  pid                 INT NOT NULL,
  name                VARCHAR(80) NOT NULL,
  total_reads	      INT,
  mapped_reads	      INT,
  duplicate_reads     INT,
  mean_isize	      float,

  KEY sid_idx  (sid),
  KEY pid_idx  (pid)
);


CREATE TABLE variant (

  vid                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  chr                 VARCHAR(8) NOT NULL ,
  pos		      INT NOT NULL,
  ref                 VARCHAR(100) NOT NULL ,
  alt                 VARCHAR(100) NOT NULL ,
  comment	      VARCHAR(200),
  annotation	      VARCHAR(500),

  KEY pos_idx  (chr, pos, ref, alt)
);


CREATE TABLE region (

  rid                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  chr                 VARCHAR(8) NOT NULL ,
  start               INT NOT NULL,
  end                 INT NOT NULL,
  name                VARCHAR(100) NOT NULL ,
  reference	      VARCHAR(100) NOT NULL,

  KEY pos_idx  (chr, start, end),
  KEY ref_idx (reference),
  KEY name_idx (name)
);

CREATE TABLE coverage (

#  cid                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ssid                INT NOT NULL,
  rid                 INT NOT NULL,

  min		      FLOAT,  
  mean		      FLOAT,  
  max		      FLOAT,  
  lows		      VARCHAR(300),
  missing	      VARCHAR(300),

  KEY ssid_idx  (ssid),
  KEY rid_idx  (rid)
);



CREATE TABLE sample_variant (

  ssid                INT NOT NULL,
  vid                 INT NOT NULL,

  depth		      INT,  
  AAF		      FLOAT,  
  quality	      FLOAT,  

  KEY ssid_idx  (ssid),
  KEY vid_idx  (vid)
);


