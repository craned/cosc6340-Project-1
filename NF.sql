SELECT COUNT(*) FROM team06schema.R 
	WHERE K1 IS NULL or K2 IS NULL

SELECT COUNT(*) FROM team06schema.R
	 GROUP BY K1, K2

SELECT K1,K2,A,B,C FROM team06schema.R

SELECT COUNT(*) FROM team06schema.S 
	WHERE K1 IS NULL

SELECT COUNT(*) FROM team06schema.S
	 GROUP BY K1

SELECT K1,A FROM team06schema.S

SELECT COUNT(*) FROM team06schema.T 
	WHERE K1 IS NULL or K2 IS NULL

SELECT COUNT(*) FROM team06schema.T
	 GROUP BY K1, K2

SELECT K1,K2,A FROM team06schema.T

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K1, COUNT(DISTINCT A) as ca FROM team06schema.T where A is not null
		 GROUP BY K1) AS derivedTable ;

SELECT COUNT(DISTINCT K1) as ck FROM team06schema.T where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct K2, COUNT(DISTINCT A) as ca FROM team06schema.T where A is not null
		 GROUP BY K2) AS derivedTable ;

SELECT COUNT(DISTINCT K2) as ck FROM team06schema.T where A is not null

SELECT COUNT(*) FROM team06schema.x 
	WHERE k1 IS NULL or k2 IS NULL or k3 IS NULL

SELECT COUNT(*) FROM team06schema.x
	 GROUP BY k1, k2, k3

SELECT k1,k2,k3,A FROM team06schema.x

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.x where A is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.x where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.x where A is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.x where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.x where A is not null
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.x where A is not null

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.x where A is not null
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.x where A is not null
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x where A is not null
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.x where A is not null
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x where A is not null
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.x where A is not null
		 GROUP BY k2,k3) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.x where A is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.x where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.x where A is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.x where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.x where A is not null
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.x where A is not null

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.x where A is not null
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.x where A is not null
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x where A is not null
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.x where A is not null
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.x where A is not null
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.x where A is not null
		 GROUP BY k2,k3) AS derived

create table team06schema.x1(k1 varchar(10) ,k2 varchar(10) ,k3 varchar(10) ,primary key(k1,k2,k3))

select team06schema.x1.k1,team06schema.x1.k2,team06schema.x1.k3,team06schema.x2.A FROM team06schema.x1 
INNER JOIN team06schema.x2 ON x1.k1=x2.k1 and x1.k2=x2.k2

SELECT COUNT(*) FROM team06schema.Y 
	WHERE k1 IS NULL or k2 IS NULL or k3 IS NULL or k4 IS NULL

SELECT COUNT(*) FROM team06schema.Y
	 GROUP BY k1, k2, k3, k4

SELECT k1,k2,k3,k4,A FROM team06schema.Y

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Y where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Y where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.Y where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k4, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k4) AS derivedTable ;

SELECT COUNT(DISTINCT k4) as ck FROM team06schema.Y where A is not null

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.Y where A is not null
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.Y where A is not null
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k4 FROM team06schema.Y where A is not null
		 GROUP BY k1,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.Y where A is not null
		 GROUP BY k2,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k4 FROM team06schema.Y where A is not null
		 GROUP BY k2,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k3,k4 FROM team06schema.Y where A is not null
		 GROUP BY k3,k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k3 FROM team06schema.Y where A is not null
		 GROUP BY k1,k2 , k3) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k4 FROM team06schema.Y where A is not null
		 GROUP BY k1,k2 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 , k4 FROM team06schema.Y where A is not null
		 GROUP BY k1,k3 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k2 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 , k4 FROM team06schema.Y where A is not null
		 GROUP BY k2,k3 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Y where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Y where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k3, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k3) AS derivedTable ;

SELECT COUNT(DISTINCT k3) as ck FROM team06schema.Y where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k4, COUNT(DISTINCT A) as ca FROM team06schema.Y where A is not null
		 GROUP BY k4) AS derivedTable ;

SELECT COUNT(DISTINCT k4) as ck FROM team06schema.Y where A is not null

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k2, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k2) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 FROM team06schema.Y where A is not null
		 GROUP BY k1,k2) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 FROM team06schema.Y where A is not null
		 GROUP BY k1,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k1 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k4 FROM team06schema.Y where A is not null
		 GROUP BY k1,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 FROM team06schema.Y where A is not null
		 GROUP BY k2,k3) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k4 FROM team06schema.Y where A is not null
		 GROUP BY k2,k4) AS derived

SELECT sum(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k3,k4 FROM team06schema.Y where A is not null
		 GROUP BY k3,k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k3, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k2 , k3) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k3 FROM team06schema.Y where A is not null
		 GROUP BY k1,k2 , k3) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k2 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k2 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k2 , k4 FROM team06schema.Y where A is not null
		 GROUP BY k1,k2 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k1 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k1,k3 , k4 FROM team06schema.Y where A is not null
		 GROUP BY k1,k3 , k4) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2 , k3 , k4, COUNT(DISTINCT A) AS ca FROM team06schema.Y where A is not null
		 GROUP BY k2 , k3 , k4) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT k2,k3 , k4 FROM team06schema.Y where A is not null
		 GROUP BY k2,k3 , k4) AS derived

create table team06schema.Y1(k1 varchar(10) ,k2 varchar(10) ,k3 varchar(10) ,k4 varchar(10) ,primary key(k1,k2,k3,k4))

select team06schema.Y1.k1,team06schema.Y1.k2,team06schema.Y1.k3,team06schema.Y1.k4,team06schema.Y2.A FROM team06schema.Y1 
INNER JOIN team06schema.Y2 ON Y1.k2=Y2.k2 and Y1.k4=Y2.k4

SELECT COUNT(*) FROM team06schema.Z 
	WHERE k1 IS NULL or k2 IS NULL

SELECT COUNT(*) FROM team06schema.Z
	 GROUP BY k1, k2

SELECT k1,k2,A,B,C,D FROM team06schema.Z

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT A) as ca FROM team06schema.Z where A is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT B) as ca FROM team06schema.Z where B is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z where B is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT C) as ca FROM team06schema.Z where C is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z where C is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k1, COUNT(DISTINCT D) as ca FROM team06schema.Z where D is not null
		 GROUP BY k1) AS derivedTable ;

SELECT COUNT(DISTINCT k1) as ck FROM team06schema.Z where D is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT A) as ca FROM team06schema.Z where A is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z where A is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT B) as ca FROM team06schema.Z where B is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z where B is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT C) as ca FROM team06schema.Z where C is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z where C is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct k2, COUNT(DISTINCT D) as ca FROM team06schema.Z where D is not null
		 GROUP BY k2) AS derivedTable ;

SELECT COUNT(DISTINCT k2) as ck FROM team06schema.Z where D is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT B) as ca FROM team06schema.Z where A is not null and B is not null
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z where A is not null and B is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT C) as ca FROM team06schema.Z where A is not null and C is not null
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z where A is not null and C is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT D) as ca FROM team06schema.Z where A is not null and D is not null
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z where A is not null and D is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT A) as ca FROM team06schema.Z where B is not null and A is not null
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z where B is not null and A is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT C) as ca FROM team06schema.Z where B is not null and C is not null
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z where B is not null and C is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT D) as ca FROM team06schema.Z where B is not null and D is not null
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z where B is not null and D is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT A) as ca FROM team06schema.Z where C is not null and A is not null
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z where C is not null and A is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT B) as ca FROM team06schema.Z where C is not null and B is not null
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z where C is not null and B is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT D) as ca FROM team06schema.Z where C is not null and D is not null
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z where C is not null and D is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT A) as ca FROM team06schema.Z where D is not null and A is not null
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z where D is not null and A is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT B) as ca FROM team06schema.Z where D is not null and B is not null
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z where D is not null and B is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT C) as ca FROM team06schema.Z where D is not null and C is not null
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z where D is not null and C is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT C) AS ca FROM team06schema.Z where A is not null and B is not null and C is not null
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z where A is not null and B is not null and C is not null
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT D) AS ca FROM team06schema.Z where A is not null and B is not null and D is not null
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z where A is not null and B is not null and D is not null
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT B) AS ca FROM team06schema.Z where A is not null and C is not null and B is not null
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z where A is not null and C is not null and B is not null
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z where A is not null and C is not null and D is not null
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z where A is not null and C is not null and D is not null
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z where A is not null and D is not null and B is not null
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z where A is not null and D is not null and B is not null
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z where A is not null and D is not null and C is not null
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z where A is not null and D is not null and C is not null
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT A) AS ca FROM team06schema.Z where B is not null and C is not null and A is not null
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z where B is not null and C is not null and A is not null
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z where B is not null and C is not null and D is not null
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z where B is not null and C is not null and D is not null
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z where B is not null and D is not null and A is not null
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z where B is not null and D is not null and A is not null
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z where B is not null and D is not null and C is not null
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z where B is not null and D is not null and C is not null
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z where C is not null and D is not null and A is not null
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z where C is not null and D is not null and A is not null
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z where C is not null and D is not null and B is not null
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z where C is not null and D is not null and B is not null
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z where A is not null and B is not null and D is not null and C is not null
		 GROUP BY A , B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , C FROM team06schema.Z where A is not null and B is not null and D is not null and C is not null
		 GROUP BY A,B , C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z where A is not null and B is not null and C is not null and D is not null
		 GROUP BY A , B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , D FROM team06schema.Z where A is not null and B is not null and C is not null and D is not null
		 GROUP BY A,B , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z where A is not null and C is not null and B is not null and D is not null
		 GROUP BY A , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C , D FROM team06schema.Z where A is not null and C is not null and B is not null and D is not null
		 GROUP BY A,C , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT B , C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z where B is not null and C is not null and A is not null and D is not null
		 GROUP BY B , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C , D FROM team06schema.Z where B is not null and C is not null and A is not null and D is not null
		 GROUP BY B,C , D) AS derived

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT B) as ca FROM team06schema.Z where A is not null and B is not null
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z where A is not null and B is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT C) as ca FROM team06schema.Z where A is not null and C is not null
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z where A is not null and C is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT A, COUNT(DISTINCT D) as ca FROM team06schema.Z where A is not null and D is not null
		 GROUP BY A) AS derivedTable ;

SELECT COUNT(DISTINCT A) AS ck FROM team06schema.Z where A is not null and D is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT A) as ca FROM team06schema.Z where B is not null and A is not null
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z where B is not null and A is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT C) as ca FROM team06schema.Z where B is not null and C is not null
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z where B is not null and C is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT B, COUNT(DISTINCT D) as ca FROM team06schema.Z where B is not null and D is not null
		 GROUP BY B) AS derivedTable ;

SELECT COUNT(DISTINCT B) AS ck FROM team06schema.Z where B is not null and D is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT A) as ca FROM team06schema.Z where C is not null and A is not null
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z where C is not null and A is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT B) as ca FROM team06schema.Z where C is not null and B is not null
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z where C is not null and B is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT C, COUNT(DISTINCT D) as ca FROM team06schema.Z where C is not null and D is not null
		 GROUP BY C) AS derivedTable ;

SELECT COUNT(DISTINCT C) AS ck FROM team06schema.Z where C is not null and D is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT A) as ca FROM team06schema.Z where D is not null and A is not null
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z where D is not null and A is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT B) as ca FROM team06schema.Z where D is not null and B is not null
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z where D is not null and B is not null

SELECT SUM(ca) AS countnonKeyToNonKey FROM 
	(SELECT DISTINCT D, COUNT(DISTINCT C) as ca FROM team06schema.Z where D is not null and C is not null
		 GROUP BY D) AS derivedTable ;

SELECT COUNT(DISTINCT D) AS ck FROM team06schema.Z where D is not null and C is not null

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT C) AS ca FROM team06schema.Z where A is not null and B is not null and C is not null
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z where A is not null and B is not null and C is not null
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , B, COUNT(DISTINCT D) AS ca FROM team06schema.Z where A is not null and B is not null and D is not null
		 GROUP BY A , B) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B FROM team06schema.Z where A is not null and B is not null and D is not null
		 GROUP BY A,B) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT B) AS ca FROM team06schema.Z where A is not null and C is not null and B is not null
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z where A is not null and C is not null and B is not null
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z where A is not null and C is not null and D is not null
		 GROUP BY A , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C FROM team06schema.Z where A is not null and C is not null and D is not null
		 GROUP BY A,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z where A is not null and D is not null and B is not null
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z where A is not null and D is not null and B is not null
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct A , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z where A is not null and D is not null and C is not null
		 GROUP BY A , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,D FROM team06schema.Z where A is not null and D is not null and C is not null
		 GROUP BY A,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT A) AS ca FROM team06schema.Z where B is not null and C is not null and A is not null
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z where B is not null and C is not null and A is not null
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z where B is not null and C is not null and D is not null
		 GROUP BY B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C FROM team06schema.Z where B is not null and C is not null and D is not null
		 GROUP BY B,C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z where B is not null and D is not null and A is not null
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z where B is not null and D is not null and A is not null
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z where B is not null and D is not null and C is not null
		 GROUP BY B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,D FROM team06schema.Z where B is not null and D is not null and C is not null
		 GROUP BY B,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z where C is not null and D is not null and A is not null
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z where C is not null and D is not null and A is not null
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT distinct C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z where C is not null and D is not null and B is not null
		 GROUP BY C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT C,D FROM team06schema.Z where C is not null and D is not null and B is not null
		 GROUP BY C,D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , C, COUNT(DISTINCT D) AS ca FROM team06schema.Z where A is not null and B is not null and D is not null and C is not null
		 GROUP BY A , B , C) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , C FROM team06schema.Z where A is not null and B is not null and D is not null and C is not null
		 GROUP BY A,B , C) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , B , D, COUNT(DISTINCT C) AS ca FROM team06schema.Z where A is not null and B is not null and C is not null and D is not null
		 GROUP BY A , B , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,B , D FROM team06schema.Z where A is not null and B is not null and C is not null and D is not null
		 GROUP BY A,B , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT A , C , D, COUNT(DISTINCT B) AS ca FROM team06schema.Z where A is not null and C is not null and B is not null and D is not null
		 GROUP BY A , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT A,C , D FROM team06schema.Z where A is not null and C is not null and B is not null and D is not null
		 GROUP BY A,C , D) AS derived

SELECT SUM(ca) AS countKeyToNonKey FROM 
	(SELECT DISTINCT B , C , D, COUNT(DISTINCT A) AS ca FROM team06schema.Z where B is not null and C is not null and A is not null and D is not null
		 GROUP BY B , C , D) AS derivedTable ;

SELECT COUNT(*) AS ck FROM 
	(SELECT B,C , D FROM team06schema.Z where B is not null and C is not null and A is not null and D is not null
		 GROUP BY B,C , D) AS derived

CREATE table team06schema.Z1(k1 varchar(10),k2 varchar(10),B varchar(10),D varchar(10),primary key(k1,k2))

select  team06schema.Z1.k1,team06schema.Z1.k2,team06schema.Z1.B,team06schema.Z1.D,team06schema.Z2.A FROM team06schema.Z1 
INNER JOIN team06schema.Z2 ON Z1.B=Z2.B

