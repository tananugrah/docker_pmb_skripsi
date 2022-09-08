class query_pararelize_services :
    select_service_dbsources = (
        """   
       with
    calon_mahasiswa_fact as (
    select distinct mhs_cln.id_pmb,
                m_agama.kode_agama,
                m_kewarganegaraan.kode_kewarganegaraan,
                m_prodi.id_prodi,
                m_fakultas.id_fakultas,
                dim_waktu.id_tahun

    from mhs_cln

    left join m_prodi on mhs_cln.prodi1 = m_prodi.id_prodi
    left join m_fakultas on m_prodi.id_fakultas = m_fakultas.id_fakultas
    left join m_agama on mhs_cln.kode_agama = m_agama.kode_agama
    left join m_kewarganegaraan on mhs_cln.warga = m_kewarganegaraan.kode_kewarganegaraan
    left join dim_waktu on mhs_cln.id_pmb = dim_waktu.id_pmb
    where mhs_cln.id_pmb != ' ' and
          mhs_cln.id_pmb is not null and
        dim_waktu.id_tahun >= 7 and
          mhs_cln.tanggal_bayar !='' and
          mhs_cln.tanggal_bayar is not null and
          mhs_cln.tanggal_bayar != '0000-00-00'
    ),
     fact_table_calon_mahasiswa as (
         select n_pmb,
                id_pmb,
                id_tahun,
                kode_agama,
                kode_kewarganegaraan,
                id_prodi,
                id_fakultas
         from (
                    select count(id_pmb) n_pmb,
                                id_pmb,
                                id_tahun,
                                kode_agama,
                           kode_kewarganegaraan,
                           id_prodi,
                           id_fakultas

                  from calon_mahasiswa_fact
                  group by 2, 3, 4, 5,6,7
              ) a
         group by n_pmb,2, 3, 4, 5, 6 ,7
     )
     select
        n_pmb,
        id_pmb,
        id_tahun,
        kode_agama,
        kode_kewarganegaraan,
        id_prodi,
        id_fakultas

     from fact_table_calon_mahasiswa
     group by n_pmb,2,3,4,5,6,7
     order by 3 desc;
        """
    )