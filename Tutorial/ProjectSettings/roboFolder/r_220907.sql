###
UPDATE swiss2022.strasse_l
SET a_class = (
CASE 
WHEN btu = 'B' THEN (SELECT OVERLAY(a_class placing '1' from 3)) 
WHEN btu = 'T' THEN (SELECT OVERLAY(a_class placing '2' from 3)) 
WHEN btu = 'U' THEN (SELECT OVERLAY(a_class placing '3' from 3))
ELSE a_class
END)
WHERE COALESCE(a_class, '') != '';