http://10.87.216.197:50070/dfshealth.html#tab-overview
http://10.87.216.166:50070/dfshealth.html#tab-overview

10.75.1.131
hdfs://10.87.216.166:8020
hdfs://10.87.216.197:8020

/user/feed_weibo/warehouse/feed_sample_json_final
/user/feed_weibo/warehouse/feed_data_v0/dt=20170521/010897_0.gz

10.85.123.43
10.85.123.46
10.85.123.44
10.85.123.45
ASDqwe123


dt=20170415 : 550525463
dt=20170416 : 447567956
dt=20170417 : 325356842
dt=20170418 : 147330297
dt=20170419 : 95117992
dt=20170420 : 266162251
dt=20170421 : 227359602
dt=20170422 : 235026443
dt=20170423 : 253776339
dt=20170424 : 231596163
dt=20170425 : 375623485
dt=20170426 : 449805698
dt=20170427 : 418655867
dt=20170428 : 420715330
dt=20170429 : 408347957
dt=20170430 : 14263978
dt=20170501 : 263788448
dt=20170502 : 200131172
dt=20170503 : 322817037
dt=20170504 : 418357589
dt=20170505 : 273533187
dt=20170506 : 420035209
dt=20170507 : 454899479
dt=20170508 : 367118373
dt=20170509 : 317365364
dt=20170510 : 436438292
dt=20170511 : 305912251
dt=20170512 : 315442451
dt=20170513 : 348328828
dt=20170514 : 330571783
dt=20170515 : 530281198
dt=20170516 : 926674981
dt=20170517 : 688794470
dt=20170518 : 1347966646
dt=20170519 : 972317328
dt=20170520 : 1143315530
dt=20170521 : 1943557312
dt=20170522 : 1107039064
dt=20170523 : 1588320804
dt=20170524 : 3106971893
dt=20170525 : 4537045687
dt=20170526 : 4482464295
dt=20170527 : 3485903961
dt=20170528 : 3558683322
dt=20170529 : 2573585784
dt=20170530 : 3282251010
dt=20170531 : 4781724264
dt=20170601 : 4955751063
dt=20170602 : 4561725903
dt=20170603 : 3819993038
dt=20170604 : 3662879622
dt=20170605 : 3475055790
dt=20170606 : 4605701150
dt=20170607 : 4278050629
dt=20170608 : 5321625879
dt=20170609 : 5826279442
dt=20170610 : 4512594573
dt=20170611 : 4638356914
dt=20170612 : 4163549449
dt=20170613 : 4842871031
dt=20170614 : 4957800768
dt=20170615 : 5017876775
dt=20170616 : 5944686617
dt=20170617 : 5116767726
dt=20170618 : 3844852660
133,245,287,704

pengyu7 s path : /data0/user/pengyu7/feed_rank/dataflow
ipMaster s path : /home/hadoop/ksp/dataflow


/user/feed_weibo/warehouse/feed_data_v0/dt=20170613
from_hdfs=/user/weibo_bigdata_ds/warehouse/feature_offline_user/dt=$latest_user_dt
to_hdfs=hdfs://10.87.216.166:8020/user/feed_weibo/warehouse/feature_offline_user/dt=$dt


/user/feed_weibo/warehouse/feed_data_v0

hdfs://10.87.216.197:8020/user/feed_weibo/warehouse/feed_data_v0/dt=20170520/007667_0.gz


# rm -rf ~/libs/hadoop-2.7.3/


rm -rf /home/hadoop/libs/hadoop-2.7.3/tmp/
rm -rf /home/hadoop/libs/hadoop-2.7.3/logs/*
rm -rf /home/hadoop/libs/hadoop-2.7.3/data/hdfs/datanode/*
rm -rf /home/hadoop/libs/hadoop-2.7.3/data/hdfs/namenode/*
scp -r /home/hadoop/libs/hadoop-2.7.3 hadoop@${ipSlaver1}:/home/hadoop/libs/hadoop-2.7.3/ &
scp -r /home/hadoop/libs/hadoop-2.7.3 hadoop@${ipSlaver2}:/home/hadoop/libs/hadoop-2.7.3/ &
scp -r /home/hadoop/libs/hadoop-2.7.3 hadoop@${ipSlaver3}:/home/hadoop/libs/hadoop-2.7.3/ &

scp -r /home/hadoop/libs/ hadoop@${ipSlaver1}:/home/hadoop/libs/
scp -r /home/hadoop/libs/ hadoop@${ipSlaver2}:/home/hadoop/libs/
scp -r /home/hadoop/libs/ hadoop@${ipSlaver3}:/home/hadoop/libs/

fdisk -l
parted /dev/vdb
mklabel gpt
mkpart ext4 0 -1
mkfs.ext4 /dev/vdb1
blkid 
vim /etc/fstab
# UUID=9d7c60a7-8642-4cf8-9b58-b1f68c1a9d52 /data                       ext4    defaults        0 2

parted
select /dev/vdb 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore
select /dev/vdc 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore
select /dev/vdd 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vde 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdf 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdg 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdh 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdi 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdj 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdk 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdl 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdm 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdn 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdo 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdp 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 
select /dev/vdq 
mklabel gpt
Yes
mkpart ext4 0 -1
Ignore 


mkfs.ext4 /dev/vdb1


mkfs.ext4 /dev/vdb1 


mkfs.ext4 /dev/vdc1 


mkfs.ext4 /dev/vdd1 


mkfs.ext4 /dev/vde1 


mkfs.ext4 /dev/vdf1 


mkfs.ext4 /dev/vdg1 


mkfs.ext4 /dev/vdh1 


mkfs.ext4 /dev/vdi1 


mkfs.ext4 /dev/vdj1 


mkfs.ext4 /dev/vdk1 


mkfs.ext4 /dev/vdl1 


mkfs.ext4 /dev/vdm1 


mkfs.ext4 /dev/vdn1 


mkfs.ext4 /dev/vdo1 


mkfs.ext4 /dev/vdp1 


mkfs.ext4 /dev/vdq1 



mkdir /datab
mkdir /datac
mkdir /datad
mkdir /datae
mkdir /dataf
mkdir /datag
mkdir /datah
mkdir /datai
mkdir /dataj
mkdir /datak
mkdir /datal
mkdir /datam
mkdir /datan
mkdir /datao
mkdir /datap
mkdir /dataq
blkid 

mount -a
# 43:
UUID=9d7c60a7-8642-4cf8-9b58-b1f68c1a9d52  /datab                       ext4    defaults        0 2
UUID=192432f3-38f0-4c02-bc83-8aafba51b47c  /datac                       ext4    defaults        0 2
UUID=aa88123f-9116-4511-96e4-f405324b31d8  /datad                       ext4    defaults        0 2
UUID=cb9a3c75-d93c-4509-ab04-2e934af00fbd  /datae                       ext4    defaults        0 2
UUID=463cc098-33ef-4661-ba5c-6fdfdfb6e275  /dataf                       ext4    defaults        0 2
UUID=b18e214d-69fe-457a-b2c0-5dbc6695005e  /datag                       ext4    defaults        0 2
UUID=1cb29195-0e7b-41c5-baf2-f6619ceb0cbd  /datah                       ext4    defaults        0 2
UUID=2f271930-9812-4ccb-9bef-1068b5bd6165  /datai                       ext4    defaults        0 2
UUID=0dcd73cd-ad51-4a58-9ad7-77b1b74a48f1  /dataj                       ext4    defaults        0 2
UUID=6cb4a598-dad8-48ae-addb-6b1f3c80b2e4  /datak                       ext4    defaults        0 2
UUID=d7433945-d66a-45b7-8162-95d55e42fee4  /datal                       ext4    defaults        0 2
UUID=9a8e0c86-f6e0-406c-8008-132ee2a8a0c4  /datam                       ext4    defaults        0 2
UUID=969874f4-1a27-499f-9f4d-35f72609b76a  /datan                       ext4    defaults        0 2
UUID=45f058ad-fe15-452b-9d42-48ca6e85e8a1  /datao                       ext4    defaults        0 2
UUID=d8c2809e-87e9-46b9-9dd1-e2f07ae51b63  /datap                       ext4    defaults        0 2
UUID=4517ca2c-f52f-470c-a556-54a0916f2174  /dataq                       ext4    defaults        0 2

# 44:
UUID=3bf4d0df-3ebd-4107-b011-90dc4a24b9c2  /datab                       ext4    defaults        0 2
UUID=0b83359f-4e90-4213-b037-854bb5aa0c59  /datac                       ext4    defaults        0 2
UUID=5f803b62-38b9-4a0f-bf90-c73f7d5062cd  /datad                       ext4    defaults        0 2
UUID=3e09d9b2-9811-435a-9032-3314633bc274  /datae                       ext4    defaults        0 2
UUID=da0fe85b-8061-4898-b374-2d6c4c1f2e89  /dataf                       ext4    defaults        0 2
UUID=d44c95a6-2ba4-449b-9a9a-dd06b3f8f057  /datag                       ext4    defaults        0 2
UUID=09481391-6c0c-4568-8231-08b645970393  /datah                       ext4    defaults        0 2
UUID=8dbfa1b4-9155-437e-84e7-ef3189582a0d  /datai                       ext4    defaults        0 2
UUID=c87f212f-ec05-417f-bc96-309c255cfe10  /dataj                       ext4    defaults        0 2
UUID=9533b3b0-d5f1-4d33-91c0-c42e76c58fa3  /datak                       ext4    defaults        0 2
UUID=51901958-ba85-46e7-8410-06718fb50526  /datal                       ext4    defaults        0 2
UUID=9b431ef4-e7ba-4c7a-b5c1-d79ad89df949  /datam                       ext4    defaults        0 2
UUID=5e3d5b5d-42a6-42ee-9857-4f1c7ab3d2d5  /datan                       ext4    defaults        0 2
UUID=3799087a-5cad-4f67-a00f-5ea14cdf5b16  /datao                       ext4    defaults        0 2
UUID=f8ac2f9c-ac38-40e8-91c2-44eda6dd9f81  /datap                       ext4    defaults        0 2
UUID=4269fdd2-0d4d-41b6-9f68-2dbe524c6a5f  /dataq                       ext4    defaults        0 2

# 45:
UUID=0edd7175-b336-4fc1-8c6e-3fa7ee558555 /datab                       ext4   defaults        0 2
UUID=aeba249d-e0b8-47d9-82af-eb1233b095b9 /datac                       ext4   defaults        0 2
UUID=d6aa2405-ba16-4c38-acaf-4153cbdd3792 /datad                       ext4   defaults        0 2
UUID=6f7b8938-18b6-4589-a739-00c30c8779d5 /datae                       ext4   defaults        0 2
UUID=ab5a8811-4c4e-43c0-8b25-c9472e0da8ac /dataf                       ext4   defaults        0 2
UUID=b3fceae6-524a-4e39-b7a1-44676e511104 /datag                       ext4   defaults        0 2
UUID=0bdde8eb-efeb-49a8-b5f8-3f0f36c98e28 /datah                       ext4   defaults        0 2
UUID=968e033e-88a7-4ab4-9526-94e5c6f73594 /datai                       ext4   defaults        0 2
UUID=482816e2-6890-4d34-93c2-cb479838fbe5 /dataj                       ext4   defaults        0 2
UUID=0a514cde-fb03-4f76-9b91-5cf61f2b18d1 /datak                       ext4   defaults        0 2
UUID=376afab7-4e87-479e-b849-9973c222dd97 /datal                       ext4   defaults        0 2
UUID=34e17d83-5506-4706-b598-df2c34f23b1f /datam                       ext4   defaults        0 2
UUID=778e5fa0-49bc-451e-a70b-4da4d66e26c7 /datan                       ext4   defaults        0 2
UUID=5f282118-b164-4bb8-a00c-ebe19a171c4c /datao                       ext4   defaults        0 2
UUID=49212b7b-0224-4062-8b86-a7beed909e63 /datap                       ext4   defaults        0 2
UUID=d1cf2c2c-0c4c-42b6-9a05-ad5b7e7f8502 /dataq                       ext4   defaults        0 2

# 46:
UUID=c869a9a5-a677-49d1-8e85-6f35cc462db0 /datab                       ext4   defaults        0 2
UUID=c87120aa-cd77-4caf-9510-520c1e726861 /datac                       ext4   defaults        0 2
UUID=a4b59191-2d70-48e3-b773-0095bd6af68a /datad                       ext4   defaults        0 2
UUID=2f0d2d8e-b1aa-4045-ac31-2d48f25f54d7 /datae                       ext4   defaults        0 2
UUID=a7894ddd-377c-4947-a7bc-99f0a9540774 /dataf                       ext4   defaults        0 2
UUID=eef779da-e11e-4c0a-a8f2-3d21110c0b3f /datag                       ext4   defaults        0 2
UUID=f68480aa-21d0-42a6-837e-38bd0e6135eb /datah                       ext4   defaults        0 2
UUID=c77adba0-50bc-47d3-bfff-a0ae38c97098 /datai                       ext4   defaults        0 2
UUID=bbd4cd27-4364-4f66-817c-06049d2b4890 /dataj                       ext4   defaults        0 2
UUID=be5047fd-6736-408f-90c7-ae53590b4853 /datak                       ext4   defaults        0 2
UUID=6f6bcf29-3122-4fd9-97d8-2b7238816ed5 /datal                       ext4   defaults        0 2
UUID=56e360f4-aa6a-48e7-a8f7-8e1f0b1562cd /datam                       ext4   defaults        0 2
UUID=791d8ef2-7d19-49e4-915e-d923c63ea01d /datan                       ext4   defaults        0 2
UUID=10869b86-0e5e-453e-a856-61bef525c0fd /datao                       ext4   defaults        0 2
UUID=13c01d80-e006-4c4e-8e1e-97baf09d19d6 /datap                       ext4   defaults        0 2
UUID=12f47dfc-404f-4767-b933-400fc6b46534 /dataq                       ext4   defaults        0 2


chmod +777  /datab 
chmod +777  /datac 
chmod +777  /datad 
chmod +777  /datae 
chmod +777  /dataf 
chmod +777  /datag 
chmod +777  /datah 
chmod +777  /datai 
chmod +777  /dataj 
chmod +777  /datak 
chmod +777  /datal 
chmod +777  /datam 
chmod +777  /datan 
chmod +777  /datao 
chmod +777  /datap 
chmod +777  /dataq 


rm -rf /home/hadoop/libs/hadoop-2.7.3/logs/*

rm -rf /datab/hadoop/tmp
rm -rf /datac/hadoop/tmp
rm -rf /datad/hadoop/tmp
rm -rf /datae/hadoop/tmp
rm -rf /dataf/hadoop/tmp
rm -rf /datag/hadoop/tmp
rm -rf /datah/hadoop/tmp
rm -rf /datai/hadoop/tmp
rm -rf /dataj/hadoop/tmp
rm -rf /datak/hadoop/tmp
rm -rf /datal/hadoop/tmp
rm -rf /datam/hadoop/tmp
rm -rf /datan/hadoop/tmp
rm -rf /datao/hadoop/tmp
rm -rf /datap/hadoop/tmp
rm -rf /dataq/hadoop/tmp

rm -rf /datab/hadoop/data/hdfs/datanode/*
rm -rf /datac/hadoop/data/hdfs/datanode/*
rm -rf /datad/hadoop/data/hdfs/datanode/*
rm -rf /datae/hadoop/data/hdfs/datanode/*
rm -rf /dataf/hadoop/data/hdfs/datanode/*
rm -rf /datag/hadoop/data/hdfs/datanode/*
rm -rf /datah/hadoop/data/hdfs/datanode/*
rm -rf /datai/hadoop/data/hdfs/datanode/*
rm -rf /dataj/hadoop/data/hdfs/datanode/*
rm -rf /datak/hadoop/data/hdfs/datanode/*
rm -rf /datal/hadoop/data/hdfs/datanode/*
rm -rf /datam/hadoop/data/hdfs/datanode/*
rm -rf /datan/hadoop/data/hdfs/datanode/*
rm -rf /datao/hadoop/data/hdfs/datanode/*
rm -rf /datap/hadoop/data/hdfs/datanode/*
rm -rf /dataq/hadoop/data/hdfs/datanode/*

rm -rf /datab/hadoop/data/hdfs/namenode/*
rm -rf /datac/hadoop/data/hdfs/namenode/*
rm -rf /datad/hadoop/data/hdfs/namenode/*
rm -rf /datae/hadoop/data/hdfs/namenode/*
rm -rf /dataf/hadoop/data/hdfs/namenode/*
rm -rf /datag/hadoop/data/hdfs/namenode/*
rm -rf /datah/hadoop/data/hdfs/namenode/*
rm -rf /datai/hadoop/data/hdfs/namenode/*
rm -rf /dataj/hadoop/data/hdfs/namenode/*
rm -rf /datak/hadoop/data/hdfs/namenode/*
rm -rf /datal/hadoop/data/hdfs/namenode/*
rm -rf /datam/hadoop/data/hdfs/namenode/*
rm -rf /datan/hadoop/data/hdfs/namenode/*
rm -rf /datao/hadoop/data/hdfs/namenode/*
rm -rf /datap/hadoop/data/hdfs/namenode/*
rm -rf /dataq/hadoop/data/hdfs/namenode/*

scp path hadoop@${ipSlaver1}:path

hadoop fs -chown -R feed_weibo:feed_weibo /user/feed_weibo

# Parameters Server
sh sbin/start-on-yarn.sh -j target/weips-rc-on-yarn-0.1-SNAPSHOT-shade.jar -n 10 -v 10240 -c 2 -d hdfs://10.85.123.43:9000/user/hadoop/



