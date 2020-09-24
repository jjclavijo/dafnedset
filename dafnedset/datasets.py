import os

from .base import CachedSaver,CachedLoader
from ._constants import DATOS_PATH

class DefaultQuerys(CachedSaver,CachedLoader):
    Q_T = "SELECT max(chiid) FROM indice_t_chiid;"

    QUERY_T = """
    SELECT max(estacion) estacion, max(iid) iid, max(ch) ch,
    array_agg(to_date(i.yymmmdd,'YYMONDD') ORDER BY to_date(i.yymmmdd,'YYMONDD') ) tiempo,
    array_agg(north::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) norte, 
    array_agg(east::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) este, 
    array_agg(up::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) altura
    FROM indice_t i
    LEFT JOIN http_tseries t USING (estacion, yymmmdd)
    JOIN indice_t_chiid USING (iid,ch)
    GROUP BY chiid HAVING chiid <@ int8range(%s,%s) AND count(*) = 61;
    """

    Q_NC = "SELECT max(chiid) FROM indice_nc_chiid;"

    QUERY_NC = """
    SELECT max(estacion) estacion, max(iid) iid, max(ch) ch,
    array_agg(to_date(i.yymmmdd,'YYMONDD') ORDER BY to_date(i.yymmmdd,'YYMONDD') ) tiempo,
    array_agg(north::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) norte, 
    array_agg(east::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) este, 
    array_agg(up::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) altura
    FROM indice_nc i
    LEFT JOIN http_tseries t USING (estacion, yymmmdd)
    JOIN indice_nc_chiid USING (iid,ch)
    GROUP BY chiid HAVING chiid <@ int8range(%s,%s) AND count(*) = 61;
    """

    Q_C = "SELECT max(iid) FROM indice_c;"

    QUERY_C = """
    SELECT a.estacion, a.sid, s.time, a.tiempo, a.norte, a.este, a.altura
    FROM
      (SELECT max(estacion) estacion, max(sid) sid,
              array_agg(to_date(i.yymmmdd,'YYMONDD') 
                  ORDER BY to_date(i.yymmmdd,'YYMONDD') ) tiempo,
              array_agg(north::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) norte, 
              array_agg(east::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) este,
              array_agg(up::real ORDER BY to_date(i.yymmmdd,'YYMONDD') ) altura
       FROM indice_c i
       LEFT JOIN http_tseries t USING (estacion, yymmmdd)
       GROUP BY iid HAVING iid <@ int8range(%s,%s) ) AS a
    JOIN usgs_sismos s ON a.sid = s.ogc_fid;
    """

    POSFILE = os.path.join(DATOS_PATH,'positive.pq')
    NEGFILE = os.path.join(DATOS_PATH,'negative.pq')
    TOTFILE = os.path.join(DATOS_PATH,'undiscri.pq')

    @classmethod
    def positive(cls,**kwargs):
        try:
            this = super().read_parquet(cls.POSFILE,**kwargs)
        except OSError:
            this = super().read_db(cls.Q_C,cls.QUERY_C,**kwargs)
        this.savefile = cls.POSFILE
        return this

    @classmethod
    def negative(cls,**kwargs):
        try:
            this = super().read_parquet(cls.NEGFILE,**kwargs)
        except OSError:
            this = super().read_db(cls.Q_NC,cls.QUERY_NC,**kwargs)
        this.savefile = cls.NEGFILE
        return this

    @classmethod
    def undiscriminated(cls,**kwargs):
        try:
            this = super().read_parquet(cls.TOTFILE,**kwargs)
        except OSError:
            this = super().read_db(cls.Q_T,cls.QUERY_T,**kwargs)
        this.savefile = cls.TOTFILE
        return this

    def write_parquet(self):
        super().write_parquet(self.savefile)
