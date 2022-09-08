with mahasiswa_fact as(
    select distinct
                    no_pendaftaran as id_pmb,
                    mhs_cln.kode_agama,
                    public.m_prodi.id_prodi,
                    public.m_fakultas.id_fakultas,
                    public.m_kewarganegaraan.kode_kewarganegaraan,
                    public.adm_provinsi.id_prov,
                    public.mhs_cln.id_tahun,
                    public.dim_waktu.umur
    from public.mhs_regis
    left join mhs_cln on mhs_regis.no_pendaftaran = mhs_cln.id_pmb
    left join public.m_agama on public.mhs_regis.no_pendaftaran = public.m_agama.kode_agama
    left join public.m_prodi on public.mhs_regis.id_prodi = public.m_prodi.id_prodi
    left join public.m_fakultas on public.m_prodi.id_fakultas = public.m_fakultas.id_fakultas
    left join public.m_kewarganegaraan on public.mhs_regis.warga = public.m_kewarganegaraan.kode_kewarganegaraan
    left join public.adm_provinsi on public.mhs_regis.prov_asal = public.adm_provinsi.kode_provinsi
    left join public.dim_waktu on public.mhs_regis.no_pendaftaran = public.dim_waktu.id_pmb
    left join public.m_tahun on public.mhs_cln.id_tahun = public.m_tahun.id_tahun
where
      no_pendaftaran !=''
    and no_pendaftaran is not null
),
     fact_table_mahasiswa_regis as (
         select n_mb,
                id_pmb,
               kode_agama,
                    id_prodi,
                    id_fakultas,
                    kode_kewarganegaraan,
                    id_prov,
                id_tahun,
                umur
         from (
                select count(id_pmb) n_mb,
                       id_pmb,
                        kode_agama,
                        id_prodi,
                        id_fakultas,
                        kode_kewarganegaraan,
                         id_prov,
                       id_tahun,
                       umur
                  from mahasiswa_fact
                  group by 2, 3, 4, 5,6,7,8,9
              ) a
         group by n_mb,2, 3, 4, 5, 6 ,7,8,9
     )
     select
            n_mb,
            id_pmb,
           kode_agama,
                    id_prodi,
                    id_fakultas,
                    kode_kewarganegaraan,
                    id_prov,
            id_tahun,
            umur
     from fact_table_mahasiswa_regis
     group by n_mb,2,3, 4,5,6,7,8,9
     order by 8 desc;

create table sandbox_bi.fakta_mahasiswa(
    n_mb bigint,
    id_pmb varchar(11) PRIMARY KEY ,
    kode_agama varchar(2),
    id_prodi float8,
    id_fakultas float8,
    kode_kewarganegaraan varchar(2),
    id_prov float8,
    id_tahun float8,
    umur float8
)


ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_dim_waktu FOREIGN KEY (id_pmb) REFERENCES public.dim_waktu (id_pmb);
ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_m_fakultas FOREIGN KEY (id_fakultas) REFERENCES public.m_fakultas (id_fakultas);
ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_m_prodi FOREIGN KEY (id_prodi) REFERENCES public.m_prodi (id_prodi);
ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_m_agama FOREIGN KEY (kode_agama) REFERENCES public.m_agama (kode_agama);
ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_m_kewarganegaraan FOREIGN KEY (kode_kewarganegaraan) REFERENCES public.m_kewarganegaraan (kode_kewarganegaraan);
ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_mhs_cln FOREIGN KEY (id_pmb) REFERENCES public.mhs_cln (id_pmb);
ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_adm_provinsi FOREIGN KEY (id_prov) REFERENCES public.adm_provinsi (id_prov);
ALTER TABLE sandbox_bi.fakta_mahasiswa
    ADD CONSTRAINT fk_m_tahun FOREIGN KEY (id_tahun) REFERENCES public.m_tahun (id_tahun);


