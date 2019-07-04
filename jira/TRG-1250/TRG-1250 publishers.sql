select distinct publisher_id
from PUBLISHER_AGG_CUSTOM
WHERE hit_date > '2017-11-01'
	AND (
		custom1 in ('article', 'audio', 'audioslideshow', 'competition', 'crossword'
			, 'data', 'gallery', 'imagecontent', 'index', 'interactive', 'live'
			, 'liveblog', 'network-front', 'podcast', 'pul', 'puzzle', 'quiz'
			, 'section', 'signup', 'tag', 'tags', 'video')
		or custom2 in ('article', 'audio', 'audioslideshow', 'competition', 'crossword'
			, 'data', 'gallery', 'imagecontent', 'index', 'interactive', 'live'
			, 'liveblog', 'network-front', 'podcast', 'pul', 'puzzle', 'quiz'
			, 'section', 'signup', 'tag', 'tags', 'video')
		or custom3 in ('article', 'audio', 'audioslideshow', 'competition', 'crossword'
			, 'data', 'gallery', 'imagecontent', 'index', 'interactive', 'live'
			, 'liveblog', 'network-front', 'podcast', 'pul', 'puzzle', 'quiz'
			, 'section', 'signup', 'tag', 'tags', 'video')
		)
;

/*
-----------------
publisher_id
-----------------
926191
9597
925249
923404
10351
923193
924204
10228
926714
10507
10352
10980
10543
926144
925221
10294
926021
8747
927162
926103
10353
8601
926491
9708
9285
925294
924763
8225
925991
10079
*/
