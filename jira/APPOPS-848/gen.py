with open('in.tsv') as f:
    content = f.readlines()

lines = [line.rstrip('\n') for line in content]

prev_seg_id = 0
prev_adv_entity_id = 0
id_list = []

base_sql = "select seg_id, adv_entity_id, seg_name from SEG_RULE where seg_id = %s and adv_entity_id in (%s)"

for l in lines:
    (seg_id,seg_name,adv_entity_id,last_modified) = l.split('\t')
    if (prev_seg_id != seg_id):
        if (prev_seg_id != 0):
            print base_sql % (prev_seg_id, ','.join(id_list))
            print "union"
        prev_seg_id = seg_id
        id_list = []

    id_list.append(adv_entity_id)
print base_sql % (prev_seg_id, ','.join(id_list))
