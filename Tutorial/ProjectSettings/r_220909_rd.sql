-- add absent columns to version 1
ALTER TABLE public.oac_horse_flat_l
    DROP COLUMN IF EXISTS ref_base,
    DROP COLUMN IF EXISTS name_base;

ALTER TABLE public.oac_horse_flat_l
    ADD COLUMN IF NOT EXISTS ref_base VARCHAR[],
    ADD COLUMN IF NOT EXISTS name_base VARCHAR[];


DO
$$
DECLARE
currec RECORD;
BEGIN
FOR currec IN SELECT * FROM work.eur_num_rd WHERE COALESCE(name, '') != ''
LOOP
	UPDATE public.oac_horse_flat_l a
	SET name_base = ARRAY_APPEND(name_base, b.name_alpst),
	ref_base = ARRAY_APPEND(ref_base, b.name_ref)
	FROM work.eur_num_rd b
	WHERE currec.name = b.name
	AND COALESCE(a.num_rd, '') != ''
	AND (SELECT
	b.name IN
	(SELECT REGEXP_SPLIT_TO_TABLE((REPLACE(TRIM(BOTH '+' FROM a.num_rd),
										  '+', ' ')), E'\\s+')));
END LOOP;
END
$$;
