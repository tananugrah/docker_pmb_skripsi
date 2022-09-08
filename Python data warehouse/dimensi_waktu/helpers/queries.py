class query_pararelize_services :
    select_service_dbsources = (
        """ 
with
 waktu as (
     select distinct
                id_pmb,
                tanggal_lahir,
                tanggal_bayar,
                tanggal_ujian,
                mhs_cln.id_tahun,
                m_tahun.tahun_akademik

            from public.mhs_cln
     left join m_tahun on mhs_cln.id_tahun = m_tahun.id_tahun
            where id_pmb != ''
                and id_pmb is not null
                and mhs_cln.id_tahun >= 7
                and tanggal_bayar != ''
                and tanggal_bayar != '--'
                and tanggal_bayar != '0000-00-00'
                and tanggal_bayar is not null
            ),
            fact_waktu as (
                select
                        id_pmb,
                        tanggal_lahir,
                        tanggal_bayar,
                        tanggal_ujian,
                        id_tahun,
                       tahun_akademik
                from (
                select
                       id_pmb,
                       TO_DATE(tanggal_lahir, 'YYYY-MM-DD') tanggal_lahir,
                       TO_DATE(tanggal_bayar, 'YYYY-MM-DD') tanggal_bayar,
                       tanggal_ujian,
                       id_tahun,
                       tahun_akademik
                from waktu
                group by id_pmb,2, 3, 4, 5,6
                    ) a
                group by id_pmb, 2, 3, 4,5,6
            )
        select
               id_pmb,
               tanggal_lahir,
               tanggal_bayar,
               tanggal_ujian,
               id_tahun,
               tahun_akademik,
               EXTRACT(YEAR FROM AGE(tanggal_bayar,tanggal_lahir)) as umur
        from fact_waktu
        group by id_pmb,2,3,4,5,6,7;

        """
    )