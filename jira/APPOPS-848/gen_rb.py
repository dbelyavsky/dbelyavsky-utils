with open('rb.tsv') as f:
    content = f.readlines()

lines = [line.rstrip('\n') for line in content]

base_sql = "update SEG_RULE set seg_name = '%s' where seg_id = %s and adv_entity_id = %s;"

for l in lines:
    (seg_id,adv_entity_id,seg_name) = l.split('\t')
    print base_sql % ( seg_name, seg_id,  adv_entity_id )
