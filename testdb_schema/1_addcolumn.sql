
ALTER TABLE dbschema.testtable ADD COLUMN description TEXT;

UPDATE dbschema.testtable SET description = name WHERE id = 1;
