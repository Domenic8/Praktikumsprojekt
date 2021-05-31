SET sql_mode = '';
SET GLOBAL event_scheduler = ON;


DROP TABLE IF EXISTS Termin;
DROP TABLE IF EXISTS Testergebnis;
DROP TABLE IF EXISTS Testzentrum;
DROP TABLE IF EXISTS Website;
DROP TABLE IF EXISTS Api;
DROP TABLE IF EXISTS Userprofil;
DROP TABLE IF EXISTS Standort;



DROP PROCEDURE IF EXISTS proc_testzentrum_suche;
DROP PROCEDURE IF EXISTS proc_userprofil_anlegen;
DROP FUNCTION IF EXISTS func_userprofil_email_kennung;
DROP PROCEDURE IF EXISTS proc_chk_testanzahl;
DROP PROCEDURE IF EXISTS proc_insert_testzentrum_api_website;

DROP TRIGGER IF EXISTS trg_userprofil_pruefen;
DROP TRIGGER IF EXISTS trg_testergebnis_gueltigkeit_setzen;
DROP TRIGGER IF EXISTS trg_verfuegbare_testanzahl;
DROP TRIGGER IF EXISTS trg_terminbuchung_sperren;
DROP TRIGGER IF EXISTS trg_testzentrum_unique_name;


DROP EVENT IF EXISTS eve_testergebnis_upd;
DROP EVENT IF EXISTS eve_testergebnis_del;


CREATE TABLE Userprofil(
  userprofil_id INTEGER PRIMARY KEY AUTO_INCREMENT,
  vorname VARCHAR(55) NOT NULL,
  nachname VARCHAR(55) NOT NULL,
  email VARCHAR(55) NOT NULL,
  passwort VARCHAR(55) NOT NULL,
  geschlecht CHAR(1) NOT NULL,
  geburtsdatum DATE NOT NULL,
  handynummer INTEGER,
  verf_testanzahl INTEGER
);


CREATE TABLE Website(
  website_id INTEGER PRIMARY KEY AUTO_INCREMENT,
  domain VARCHAR(55) NOT NULL
);


CREATE TABLE Api(
   api_id INTEGER PRIMARY KEY AUTO_INCREMENT,
   link VARCHAR(55) NOT NULL
);


CREATE TABLE Testzentrum(
  testzentrum_id INTEGER PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(55) NOT NULL,
  laengengrad FLOAT,
  breitengrad FLOAT,
  oeffnung_von TIMESTAMP,
  oeffnung_bis TIMESTAMP,
  telefonnummer VARCHAR(55) NOT NULL,
  email VARCHAR(55),
  website_id INTEGER NOT NULL,
  api_id INTEGER NOT NULL,
  CONSTRAINT website_fk FOREIGN KEY (website_id) REFERENCES Website(website_id),
  CONSTRAINT api_fk FOREIGN KEY (api_id) REFERENCES Api(api_id)
);

CREATE TABLE Termin(
  termin_id INTEGER PRIMARY KEY AUTO_INCREMENT,
  zeit TIMESTAMP NOT NULL,
  status VARCHAR(55) NOT NULL,
  testzentrum_id INTEGER NOT NULL,
  userprofil_id INTEGER NOT NULL,
  CONSTRAINT testzentrum_fk FOREIGN KEY (testzentrum_id) REFERENCES Testzentrum(testzentrum_id),
  CONSTRAINT userprofil_fk FOREIGN KEY (userprofil_id) REFERENCES Userprofil(userprofil_id)
);


CREATE TABLE Testergebnis(
  testergebnis_id INTEGER PRIMARY KEY AUTO_INCREMENT,
  status VARCHAR(55) NOT NULL,
  gueltig_von TIMESTAMP,
  gueltig_bis TIMESTAMP,
  gueltigkeit VARCHAR(55) NOT NULL,
  userprofil_id INTEGER NOT NULL,
  testzentrum_id INTEGER NOT NULL,
  CONSTRAINT userprofil_fk2 FOREIGN KEY (userprofil_id) REFERENCES Userprofil(userprofil_id),
  CONSTRAINT testzentrum_fk2 FOREIGN KEY (testzentrum_id) REFERENCES Testzentrum(testzentrum_id)
);


CREATE TABLE Standort(
  standort_id INTEGER PRIMARY KEY AUTO_INCREMENT,
  land VARCHAR(55),
  stadt VARCHAR(55),
  plz INTEGER,
  strasse VARCHAR(55),
  nummer INTEGER
);


CREATE OR REPLACE VIEW v_userprofil AS
SELECT vorname, nachname, email
FROM Userprofil;

CREATE OR REPLACE VIEW v_terminensicht AS
SELECT t.zeit, t.status, u.vorname, u.nachname, tz.name, s.strasse, s.nummer
FROM Userprofil u, Termin t, Testzentrum tz, Standort s
WHERE t.status = 'gebucht';



#### Prozeduren und Funktionen ####


# Prozedur zum Anlegen eines Userprofils
DELIMITER //
CREATE PROCEDURE proc_userprofil_anlegen (p_vorname VARCHAR(55), p_nachname VARCHAR(55), p_email VARCHAR(55), p_passwort VARCHAR(55), p_geschlecht VARCHAR(55), p_geburtsdatum VARCHAR(55), p_handynummer INTEGER)
BEGIN
    INSERT INTO Userprofil(userprofil_id, vorname, nachname, email, passwort, geschlecht, geburtsdatum, handynummer)
        VALUES(NULL, p_vorname, p_nachname, p_email, p_passwort, p_geschlecht, p_geburtsdatum, p_handynummer);
    SELECT ('Profil wurde Erfolgreich angelegt.');
END //
DELIMITER ;


/* Mit der Prozedur wird anhand der Koordinaten des Users und der übergegebenen maximal Distanz
  alle Teszentren, die diese maximal Distanz zu den Koordianten Users einhalten, ausgegeben*/
DELIMITER //
CREATE PROCEDURE proc_testzentrum_suche(IN p_lgrad FLOAT, p_bgrad FLOAT, p_maxdistanz INTEGER)
BEGIN
	SELECT tz.name, (
		6137 * acos (
		cos ( radians(p_bgrad) )
		* cos( radians( tz.breitengrad ) )
		* cos( radians( tz.laengengrad ) - radians(p_lgrad) )
		+ sin ( radians(p_bgrad) )
		* sin( radians( tz.breitengrad) ))
	) AS distanz /*Berechnung der Distanz zwischen den Koordianten des Users und den Testzentren, die innerhalb der Datenbank existieren*/
    FROM Testzentrum tz
	HAVING distanz < p_maxdistanz /*Maximal Distanz für die Testzentren angezeigt werden sollen*/
	ORDER BY distanz;
END //
DELIMITER ;


# Testanzahl prüfen, wenn die Testanzahl größer oder gleich 2 ist, wird die Testanmeldung gesperrt. Ansonsten bleibt die Testanmeldung offen.
DELIMITER //
CREATE PROCEDURE proc_chk_testanzahl(id INT)
BEGIN
    DECLARE anzahl INT;
    SELECT COUNT(*) INTO anzahl
    FROM Testergebnis WHERE userprofil_id = id;
    IF anzahl >= 2 THEN
            SELECT'Testanmeldung gesperrt';
    ELSE
            SELECT' Testanmeldung offen';
    END IF;
    END //
DELIMITER ;


DELIMITER //
CREATE FUNCTION func_userprofil_email_kennung(email VARCHAR(55))
RETURNS VARCHAR(55)
BEGIN
	DECLARE v_userprofil_id INT  DEFAULT  0;

	SELECT userprofil_id INTO v_userprofil_id FROM Userprofil WHERE Userprofil.Email = email;
	IF email NOT LIKE '%@%__%.%' THEN
		RETURN 'Email in Tabelle Userprofil nicht valide!';
    ELSE
		RETURN CONCAT('Diese Email gehoert zu Userprofil ', IFNULL(v_userprofil_id, ''));
    END IF;
END;
//
DELIMITER ;


# Prozedur zur Verbindung zwischen Testzentrum und API/Website
DELIMITER //
CREATE PROCEDURE proc_insert_testzentrum_api_website (IN p_name VARCHAR(55), p_telefonnummer INTEGER, p_email VARCHAR(55), p_link VARCHAR(55), p_website_domain VARCHAR(55))
BEGIN
	DECLARE v_api INTEGER;
    DECLARE v_api_count INTEGER;
    DECLARE v_website INTEGER;
    DECLARE v_website_count INTEGER;

    # API
    SELECT count(*) INTO v_api_count FROM Api WHERE Api.link = p_link;
    IF v_api_count < 1 THEN
		INSERT INTO Api (link) VALUES (p_link);
	END IF;

    SELECT API_ID INTO v_api FROM Api WHERE Api.link = p_link;

    # Website
    SELECT count(*) INTO v_website_count FROM Website WHERE Website.domain = p_website_domain;
    IF v_website_count < 1 THEN
		INSERT INTO Website (domain) VALUES (p_website_domain);
    END IF;

    SELECT website_id INTO v_website FROM Website WHERE Website.domain = p_website_domain;

    INSERT INTO Testzentrum (name, telefonnummer, email, website_id, api_id) VALUES (p_name, p_telefonnummer, p_email, v_website, v_api);
END //
DELIMITER ;




#### Trigger ####


# Der Trigger prueft, ob die Email beim Anlegen eines Userprofils bereits vorhanden ist.
DELIMITER //
CREATE TRIGGER trg_userprofil_pruefen
BEFORE INSERT ON Userprofil
FOR EACH ROW
BEGIN
    DECLARE v_email VARCHAR(55);
    DECLARE finished INT DEFAULT 0;
    DECLARE up_cursor CURSOR FOR SELECT email FROM Userprofil;
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET finished = 1;
    OPEN up_cursor;
        REPEAT
            FETCH up_cursor INTO  v_email;
            IF NOT finished THEN
                IF NEW.email = v_email THEN
                    signal sqlstate '20001' set message_text = 'Diese Email wird bereits verwendet.';
                END IF;
            END IF;
            UNTIl finished END REPEAT;
    CLOSE up_cursor;
END //
DELIMITER ;


/*Der Trigger setzt den Gueltigkeitszietraum eines negativ Testergebnis auf ein Zeitraum von einem Tag und
  bei einem positiv Testergebnis soll ein Gueltigkeitszeitraum von 14 Tagen gesetzt werden.
  Bei Eingaben in Feld status, die weder "negativ" noch "positiv" sind, wird eine ERROR Nachricht ausgeworfen */
DELIMITER //
CREATE TRIGGER trg_testergebnis_gueltigkeit_setzen
BEFORE INSERT ON Testergebnis
FOR EACH ROW
BEGIN
	SET NEW.gueltig_von = CURRENT_TIMESTAMP; -- "gueltig_von" für jedes neue Testergebnis auf die aktuelle Systemzeit setzen
	IF NEW.status = 'negativ' THEN
		SET NEW.gueltig_bis = CURRENT_TIMESTAMP + INTERVAL '1' DAY; # negative Testergebnisse sind 1 Tag gültig
	ELSEIF NEW.status = 'positiv' THEN
    SET NEW.gueltig_bis = CURRENT_TIMESTAMP + INTERVAL '14' DAY; # positive Testergebnisse sind 14 Tage gültig
	ELSE
		signal sqlstate '20011' set message_text = 'Testergebnis kann nur positiv oder negativ sein.';
	END IF;
END //
DELIMITER ;


# Das Event soll jede 30 Minuten prüfen, ob die Gueltigkeit von negativen Testergebnissen abgelaufen ist und diese auf ungültig setzen
CREATE EVENT eve_testergebnis_upd
	ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 30 MINUTE
    DO
		UPDATE Testergebnis SET gueltigkeit = 'ungueltig' WHERE status = 'negativ' AND CURRENT_TIMESTAMP > gueltig_bis;


# Das Event soll jede 30 Minuten prüfen, ob negative Testergebnisse länger als 7 Tage abgelaufen sind und sie aus der Datenbank loeschen
CREATE EVENT eve_testergebnis_del
	ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 30 MINUTE
    DO
		DELETE FROM Testergebnis WHERE status = 'negativ' AND CURRENT_TIMESTAMP > gueltig_bis + INTERVAL '7' DAY;



# Trigger der den User benachrichtigt wie viele Test er noch offen hat.
DELIMITER //
CREATE TRIGGER trg_verfuegbare_testanzahl
BEFORE INSERT ON Testergebnis
FOR EACH ROW
BEGIN
    DECLARE v_anzahl INT;
    SELECT COUNT(*) INTO v_anzahl FROM Testergebnis WHERE userprofil_id = NEW.userprofil_id;
    IF v_anzahl = 2 THEN
        UPDATE Userprofil
		SET verf_testanzahl = 0 WHERE userprofil_id = NEW.userprofil_id;
    ELSEIF v_anzahl = 1 THEN
        UPDATE Userprofil
		SET verf_testanzahl = 1 WHERE userprofil_id = NEW.userprofil_id;
    ELSE
        UPDATE Userprofil
		SET verf_testanzahl = 2 WHERE userprofil_id = NEW.userprofil_id;
    END IF;
END //
DELIMITER ;


# Trigger der die Anmeldung zum Buchung eines Termines sperrt, wenn die verfuegbare Testanzahl gleich 0 ist
DELIMITER //
CREATE TRIGGER trg_terminbuchung_sperren
BEFORE INSERT ON Termin
FOR EACH ROW
BEGIN
	 DECLARE v_verf_testanzahl INT;
   SELECT verf_testanzahl INTO v_verf_testanzahl FROM Userprofil WHERE NEW.userprofil_id = userprofil_id;

	 IF v_verf_testanzahl = 0  THEN
		 signal sqlstate '20002' set message_text = 'Keine weitere Terminbuchung möglich';
   END IF;
END //
DELIMITER ;


# Trigger sorgt dafür, dass alle Testzentren einzigartig sein müssen
DELIMITER //
CREATE TRIGGER trg_testzentrum_unique_name
BEFORE INSERT ON Testzentrum
FOR EACH ROW
BEGIN
	DECLARE v_count INTEGER;
	Select count(*) INTO v_count FROM Testzentrum WHERE name = NEW.name;
    IF v_count > 0 THEN
		SIGNAL SQLSTATE '20003' SET MESSAGE_TEXT = 'TESTZENTRUM BEREITS VORHANDEN';
	END IF;
END //
DELIMITER ;
