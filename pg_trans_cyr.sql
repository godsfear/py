CREATE OR REPLACE FUNCTION cyrillic_transliterate(txt TEXT,gost BOOLEAN) RETURNS CHARACTER VARYING AS $BODY$
	BEGIN
		IF gost THEN
			RETURN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRANSLATE(LOWER(txt),'абвгдеёзийклмнопрстуфхцэы','abvgdeezijklmnoprstufхcey'),'ж','zh'),'ч','ch'),'ш','sh'),'щ','shh'),'ъ',''), 'ю','yu'),'я','ya'),'ь','');
		ELSE
			RETURN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRANSLATE(LOWER(txt),'абвгдеёзийклмнопрстуфыэ','abvgdeeziiklmnoprstufye'),'ж','zh'),'х','kh'),'ц','ts'),'ч','ch'),'ш','sh'),'щ','shch'),'ъ','ie'), 'ю','iu'),'я','ia'),'ь','');
		END IF;
	END;
$BODY$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION fuzzy_comp(txt1 TEXT,txt2 TEXT) RETURNS NUMERIC AS $BODY$
	DECLARE
		arr NUMERIC[];
		rez NUMERIC;
	BEGIN
		arr := array_append(arr,jaro_winkler(dmetaphone(cyrillic_transliterate(txt1,FALSE)),dmetaphone(cyrillic_transliterate(txt2,FALSE)))::NUMERIC);
		arr := array_append(arr,jaro_winkler(dmetaphone_alt(cyrillic_transliterate(txt1,FALSE)),dmetaphone_alt(cyrillic_transliterate(txt2,FALSE)))::NUMERIC);
		arr := array_append(arr,jaro_winkler(dmetaphone(cyrillic_transliterate(txt1,FALSE)),dmetaphone_alt(cyrillic_transliterate(txt2,FALSE)))::NUMERIC);
		arr := array_append(arr,jaro_winkler(dmetaphone_alt(cyrillic_transliterate(txt1,FALSE)),dmetaphone(cyrillic_transliterate(txt2,FALSE)))::NUMERIC);
		SELECT MAX(x) FROM UNNEST(arr) AS x INTO rez;
		RETURN rez;
	END;
$BODY$ LANGUAGE plpgsql VOLATILE;
