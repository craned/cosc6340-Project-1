SELECT COUNT(*) FROM team06schema.R 
WHERE K1 IS NULL or K2 IS NULL

SELECT COUNT(*) FROM team06schema.R
	 GROUP BY K1, K2

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K1, COUNT(DISTINCT A) as ca FROM team06schema.R
		 GROUP BY K1) AS derivedTable ;

SELECT COUNT(DISTINCT K1) as ck FROM team06schema.R

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K1, COUNT(DISTINCT B) as ca FROM team06schema.R
		 GROUP BY K1) AS derivedTable ;

SELECT COUNT(DISTINCT K1) as ck FROM team06schema.R

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K2, COUNT(DISTINCT A) as ca FROM team06schema.R
		 GROUP BY K2) AS derivedTable ;

SELECT COUNT(DISTINCT K2) as ck FROM team06schema.R

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K2, COUNT(DISTINCT B) as ca FROM team06schema.R
		 GROUP BY K2) AS derivedTable ;

SELECT COUNT(DISTINCT K2) as ck FROM team06schema.R

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K1, COUNT(DISTINCT A) as ca FROM team06schema.R
		 GROUP BY K1) AS derivedTable ;

SELECT COUNT(DISTINCT K1) as ck FROM team06schema.R

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K1, COUNT(DISTINCT B) as ca FROM team06schema.R
		 GROUP BY K1) AS derivedTable ;

SELECT COUNT(DISTINCT K1) as ck FROM team06schema.R

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K2, COUNT(DISTINCT A) as ca FROM team06schema.R
		 GROUP BY K2) AS derivedTable ;

SELECT COUNT(DISTINCT K2) as ck FROM team06schema.R

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K2, COUNT(DISTINCT B) as ca FROM team06schema.R
		 GROUP BY K2) AS derivedTable ;

SELECT COUNT(DISTINCT K2) as ck FROM team06schema.R

CREATE table R2(K1 varchar(10) ,B varchar(10) ,primary key(K1))

CREATE table R3(K2 varchar(10) ,A varchar(10) ,primary key(K2))

CREATE table R4(K2 varchar(10) ,B varchar(10) ,primary key(K2))

CREATE table R1(K1 varchar(10) ,K2 varchar(10) ,primary key(K1,K2))

INSERT INTO  R2(K1,B)  SELECT K1.R,B.R FROM  R

INSERT INTO  R3(K2,A)  SELECT K2.R,A.R FROM  R

INSERT INTO  R4(K2,B)  SELECT K2.R,B.R FROM  R

INSERT INTO  R1(K1,K2,  SELECT K1.R,K2.R, FROM  R

SELECT COUNT(*) FROM team06schema.S 
WHERE K1 IS NULL

SELECT COUNT(*) FROM team06schema.S
	 GROUP BY K1

SELECT COUNT(*) FROM team06schema.T 
WHERE K1 IS NULL or K2 IS NULL

SELECT COUNT(*) FROM team06schema.T
	 GROUP BY K1, K2

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K1, COUNT(DISTINCT A) as ca FROM team06schema.T
		 GROUP BY K1) AS derivedTable ;

SELECT COUNT(DISTINCT K1) as ck FROM team06schema.T

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K2, COUNT(DISTINCT A) as ca FROM team06schema.T
		 GROUP BY K2) AS derivedTable ;

SELECT COUNT(DISTINCT K2) as ck FROM team06schema.T

SELECT COUNT(*) FROM team06schema.x 
WHERE k1 IS NULL or k2 IS NULL or k3 IS NULL

SELECT COUNT(*) FROM team06schema.x
	 GROUP BY k1, k2, k3

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.x
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.x

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.x
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.x

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.x
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.x

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.x
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.x
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.x
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.x
		 GROUP BY k2,k3) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.x
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.x

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.x
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.x

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.x
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.x

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.x
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.x
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.x
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.x
		 GROUP BY k2,k3) AS derived

CREATE table R2(K1 varchar(10) ,B varchar(10) ,primary key(K1))

CREATE table R3(K2 varchar(10) ,A varchar(10) ,primary key(K2))

CREATE table R4(K2 varchar(10) ,B varchar(10) ,primary key(K2))

CREATE table x1(k1 varchar(10) ,k2 varchar(10) ,k3 varchar(10) ,primary key(k1,k2,k3))

CREATE table x2(k1 varchar(10) ,k2 varchar(10) ,A varchar(10) ,primary key(k1,k2))

CREATE table T1(K1 varchar(10) ,K2 varchar(10) ,primary key(K1,K2))

CREATE table R1(K1 varchar(10) ,K2 varchar(10) ,primary key(K1,K2))

INSERT INTO  R2(K1,B)  SELECT K1.x,B.x FROM  x

INSERT INTO  R3(K2,A)  SELECT K2.x,A.x FROM  x

INSERT INTO  R4(K2,B)  SELECT K2.x,B.x FROM  x

INSERT INTO  x1(k1,k2,k3,  SELECT k1.x,k2.x,k3.x, FROM  x

INSERT INTO  x2(k1,k2,A)  SELECT k1.x,k2.x,A.x FROM  x

INSERT INTO  T1(K1,K2,  SELECT K1.x,K2.x, FROM  x

INSERT INTO  R1(K1,K2,  SELECT K1.x,K2.x, FROM  x

SELECT COUNT(*) FROM team06schema.Y 
WHERE k1 IS NULL or k2 IS NULL or k3 IS NULL or k4 IS NULL

SELECT COUNT(*) FROM team06schema.Y
	 GROUP BY k1, k2, k3, k4

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Y

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Y

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.Y

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k4, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k4) AS derivedTable ;

SELECT COUNT(DISTINCT k4) as ck FROM team06schema.Y

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.Y
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.Y
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k4 FROM team06schema.Y
		 GROUP BY k1,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.Y
		 GROUP BY k2,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k4 FROM team06schema.Y
		 GROUP BY k2,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k3,k4 FROM team06schema.Y
		 GROUP BY k3,k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k3 FROM team06schema.Y
		 GROUP BY k1,k2 , k3) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k4 FROM team06schema.Y
		 GROUP BY k1,k2 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 , k4 FROM team06schema.Y
		 GROUP BY k1,k3 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k2 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 , k4 FROM team06schema.Y
		 GROUP BY k2,k3 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Y

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Y

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.Y

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k4, COUNT(DISTINCT A) as ca FROM team06schema.Y
		 GROUP BY k4) AS derivedTable ;

SELECT COUNT(DISTINCT k4) as ck FROM team06schema.Y

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.Y
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.Y
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k4 FROM team06schema.Y
		 GROUP BY k1,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.Y
		 GROUP BY k2,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k4 FROM team06schema.Y
		 GROUP BY k2,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k3,k4 FROM team06schema.Y
		 GROUP BY k3,k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k3 FROM team06schema.Y
		 GROUP BY k1,k2 , k3) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k4 FROM team06schema.Y
		 GROUP BY k1,k2 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k1 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 , k4 FROM team06schema.Y
		 GROUP BY k1,k3 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y
		 GROUP BY k2 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 , k4 FROM team06schema.Y
		 GROUP BY k2,k3 , k4) AS derived

CREATE table R2(K1 varchar(10) ,B varchar(10) ,primary key(K1))

CREATE table R3(K2 varchar(10) ,A varchar(10) ,primary key(K2))

CREATE table R4(K2 varchar(10) ,B varchar(10) ,primary key(K2))

CREATE table Y1(k1 varchar(10) ,k2 varchar(10) ,k3 varchar(10) ,k4 varchar(10) ,primary key(k1,k2,k3,k4))

CREATE table x1(k1 varchar(10) ,k2 varchar(10) ,k3 varchar(10) ,primary key(k1,k2,k3))

CREATE table Y2(k2 varchar(10) ,k4 varchar(10) ,A varchar(10) ,primary key(k2,k4))

CREATE table x2(k1 varchar(10) ,k2 varchar(10) ,A varchar(10) ,primary key(k1,k2))

CREATE table Y3(k1 varchar(10) ,k2 varchar(10) ,k4 varchar(10) ,A varchar(10) ,primary key(k1,k2,k4))

CREATE table Y4(k2 varchar(10) ,k3 varchar(10) ,k4 varchar(10) ,A varchar(10) ,primary key(k2,k3,k4))

CREATE table T1(K1 varchar(10) ,K2 varchar(10) ,primary key(K1,K2))

CREATE table R1(K1 varchar(10) ,K2 varchar(10) ,primary key(K1,K2))

INSERT INTO  R2(K1,B)  SELECT K1.Y,B.Y FROM  Y

INSERT INTO  R3(K2,A)  SELECT K2.Y,A.Y FROM  Y

INSERT INTO  R4(K2,B)  SELECT K2.Y,B.Y FROM  Y

INSERT INTO  Y1(k1,k2,k3,k4,  SELECT k1.Y,k2.Y,k3.Y,k4.Y, FROM  Y

INSERT INTO  x1(k1,k2,k3,  SELECT k1.Y,k2.Y,k3.Y, FROM  Y

INSERT INTO  Y2(k2,k4,A)  SELECT k2.Y,k4.Y,A.Y FROM  Y

INSERT INTO  x2(k1,k2,A)  SELECT k1.Y,k2.Y,A.Y FROM  Y

INSERT INTO  Y3(k1,k2,k4,A)  SELECT k1.Y,k2.Y,k4.Y,A.Y FROM  Y

INSERT INTO  Y4(k2,k3,k4,A)  SELECT k2.Y,k3.Y,k4.Y,A.Y FROM  Y

INSERT INTO  T1(K1,K2,  SELECT K1.Y,K2.Y, FROM  Y

INSERT INTO  R1(K1,K2,  SELECT K1.Y,K2.Y, FROM  Y

SELECT COUNT(*) FROM team06schema.Z 
WHERE k1 IS NULL or k2 IS NULL

SELECT COUNT(*) FROM team06schema.Z
	 GROUP BY k1, k2

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY A , B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , C FROM team06schema.Z
		 GROUP BY A,B , C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY A , B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , D FROM team06schema.Z
		 GROUP BY A,B , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY A , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C , D FROM team06schema.Z
		 GROUP BY A,C , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT B , C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY B , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C , D FROM team06schema.Z
		 GROUP BY B,C , D) AS derived

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT D) as ca FROM team06schema.Z
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT A) as ca FROM team06schema.Z
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT B) as ca FROM team06schema.Z
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT C) as ca FROM team06schema.Z
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z
		 GROUP BY A , B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , C FROM team06schema.Z
		 GROUP BY A,B , C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z
		 GROUP BY A , B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , D FROM team06schema.Z
		 GROUP BY A,B , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z
		 GROUP BY A , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C , D FROM team06schema.Z
		 GROUP BY A,C , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT B , C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z
		 GROUP BY B , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C , D FROM team06schema.Z
		 GROUP BY B,C , D) AS derived

CREATE table R2(K1 varchar(10) ,B varchar(10) ,primary key(K1))

CREATE table R3(K2 varchar(10) ,A varchar(10) ,primary key(K2))

CREATE table R4(K2 varchar(10) ,B varchar(10) ,primary key(K2))

CREATE table Z1(k1 varchar(10) ,k2 varchar(10) ,C varchar(10) ,D varchar(10) ,primary key(k1,k2))

CREATE table Y1(k1 varchar(10) ,k2 varchar(10) ,k3 varchar(10) ,k4 varchar(10) ,primary key(k1,k2,k3,k4))

CREATE table x1(k1 varchar(10) ,k2 varchar(10) ,k3 varchar(10) ,primary key(k1,k2,k3))

CREATE table Y2(k2 varchar(10) ,k4 varchar(10) ,A varchar(10) ,primary key(k2,k4))

CREATE table x2(k1 varchar(10) ,k2 varchar(10) ,A varchar(10) ,primary key(k1,k2))

CREATE table Y3(k1 varchar(10) ,k2 varchar(10) ,k4 varchar(10) ,A varchar(10) ,primary key(k1,k2,k4))

CREATE table Y4(k2 varchar(10) ,k3 varchar(10) ,k4 varchar(10) ,A varchar(10) ,primary key(k2,k3,k4))

CREATE table T1(K1 varchar(10) ,K2 varchar(10) ,primary key(K1,K2))

CREATE table R1(k1 varchar(10) ,k2 varchar(10) ,primary key(k1,k2))

INSERT INTO  R2(K1,B)  SELECT K1.Z,B.Z FROM  Z

INSERT INTO  R3(K2,A)  SELECT K2.Z,A.Z FROM  Z

INSERT INTO  R4(K2,B)  SELECT K2.Z,B.Z FROM  Z

INSERT INTO  Z1(k1,k2,C,D)  SELECT k1.Z,k2.Z,C.Z,D.Z FROM  Z

INSERT INTO  Y1(k1,k2,k3,k4,  SELECT k1.Z,k2.Z,k3.Z,k4.Z, FROM  Z

INSERT INTO  x1(k1,k2,k3,  SELECT k1.Z,k2.Z,k3.Z, FROM  Z

INSERT INTO  Y2(k2,k4,A)  SELECT k2.Z,k4.Z,A.Z FROM  Z

INSERT INTO  x2(k1,k2,A)  SELECT k1.Z,k2.Z,A.Z FROM  Z

INSERT INTO  Y3(k1,k2,k4,A)  SELECT k1.Z,k2.Z,k4.Z,A.Z FROM  Z

INSERT INTO  Y4(k2,k3,k4,A)  SELECT k2.Z,k3.Z,k4.Z,A.Z FROM  Z

INSERT INTO  T1(K1,K2,  SELECT K1.Z,K2.Z, FROM  Z

INSERT INTO  R1(k1,k2,  SELECT k1.Z,k2.Z, FROM  Z

