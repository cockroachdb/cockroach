select
        phone0_.id as id1_6_0_,
        person1_.id as id1_4_1_,
        phone0_.phone_number as phone_nu2_6_0_,
        phone0_.person_id as person_i4_6_0_,
        phone0_.phone_type as phone_ty3_6_0_,
        addresses2_.Person_id as Person_i1_5_0__,
        addresses2_.addresses as addresse2_5_0__,
        addresses2_.addresses_KEY as addresse3_0__,
        person1_.address as address2_4_1_,
        person1_.createdOn as createdO3_4_1_,
        person1_.name as name4_4_1_,
        person1_.nickName as nickName5_4_1_,
        person1_.version as version6_4_1_,
        addresses2_.Person_id as Person_i1_5_0__,
        addresses2_.addresses as addresse2_5_0__,
        addresses2_.addresses_KEY as addresse3_0__ 
    from
        Phone phone0_ 
    inner join
        Person person1_ 
            on phone0_.person_id=person1_.id 
    inner join
        Person_addresses addresses2_ 
            on person1_.id=addresses2_.Person_id 
where
        exists (
            select
                calls3_.id 
            from
                phone_call calls3_ 
            where
                phone0_.id=calls3_.phone_id
        )
