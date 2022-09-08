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

create table sandbox_bi.fakta_calon_mahasiswa(
    n_pmb bigint,
    id_pmb varchar(11) PRIMARY KEY ,
    id_tahun float8,
    kode_agama varchar(2),
    kode_kewarganegaraan varchar(2),
    id_prodi float8,
    id_fakultas float8
)


ALTER TABLE public.m_agama ADD PRIMARY KEY (kode_agama);
ALTER TABLE public.m_fakultas ADD PRIMARY KEY (id_fakultas);
ALTER TABLE public.m_prodi ADD PRIMARY KEY (id_prodi);
ALTER TABLE public.m_kewarganegaraan ADD PRIMARY KEY (kode_kewarganegaraan);
ALTER TABLE public.mhs_cln ADD PRIMARY KEY (id_pmb);
ALTER TABLE public.adm_provinsi ADD PRIMARY KEY (id_prov);
ALTER TABLE public.m_tahun ADD PRIMARY KEY (id_tahun);

ALTER TABLE sandbox_bi.fakta_calon_mahasiswa
    ADD CONSTRAINT fk_dim_waktu FOREIGN KEY (id_pmb) REFERENCES public.dim_waktu (id_pmb);
ALTER TABLE sandbox_bi.fakta_calon_mahasiswa
    ADD CONSTRAINT fk_m_fakultas FOREIGN KEY (id_fakultas) REFERENCES public.m_fakultas (id_fakultas);
ALTER TABLE sandbox_bi.fakta_calon_mahasiswa
    ADD CONSTRAINT fk_m_prodi FOREIGN KEY (id_prodi) REFERENCES public.m_prodi (id_prodi);
ALTER TABLE sandbox_bi.fakta_calon_mahasiswa
    ADD CONSTRAINT fk_m_agama FOREIGN KEY (kode_agama) REFERENCES public.m_agama (kode_agama);
ALTER TABLE sandbox_bi.fakta_calon_mahasiswa
    ADD CONSTRAINT fk_m_kewarganegaraan FOREIGN KEY (kode_kewarganegaraan) REFERENCES public.m_kewarganegaraan (kode_kewarganegaraan);
ALTER TABLE sandbox_bi.fakta_calon_mahasiswa
    ADD CONSTRAINT fk_mhs_cln FOREIGN KEY (id_pmb) REFERENCES public.mhs_cln (id_pmb);





