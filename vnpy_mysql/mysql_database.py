from datetime import datetime

from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    DoubleField,
    IntegerField,
    Model,
    MySQLDatabase as PeeweeMySQLDatabase,
    ModelSelect,
    ModelDelete,
    chunked,
    fn,
    Asc,
    Desc
)
from playhouse.shortcuts import ReconnectMixin

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


class ReconnectMySQLDatabase(ReconnectMixin, PeeweeMySQLDatabase):
    """带有重连混入的MySQL数据库类"""
    pass

db = ReconnectMySQLDatabase(
    database=SETTINGS["database.database"],
    user=SETTINGS["database.user"],
    password=SETTINGS["database.password"],
    host=SETTINGS["database.host"],
    port=SETTINGS["database.port"]
)


class DateTimeMillisecondField(DateTimeField):
    """支持毫秒的日期时间戳字段"""

    def get_modifiers(self) -> list:
        """毫秒支持"""
        return [3]


class DbBarData(Model):
    """K线数据表映射对象"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    datetime: DateTimeField = DateTimeField()
    interval: CharField = CharField()

    volume: DoubleField = DoubleField()
    turnover: DoubleField = DoubleField()
    open_interest: DoubleField = DoubleField()
    open_price: DoubleField = DoubleField()
    high_price: DoubleField = DoubleField()
    low_price: DoubleField = DoubleField()
    close_price: DoubleField = DoubleField()

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange", "interval", "datetime"), True),)


class DbTickData(Model):
    """TICK数据表映射对象"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    datetime: DateTimeField = DateTimeMillisecondField()

    name: CharField = CharField()
    volume: DoubleField = DoubleField()
    turnover: DoubleField = DoubleField()
    open_interest: DoubleField = DoubleField()
    last_price: DoubleField = DoubleField()
    last_volume: DoubleField = DoubleField()
    limit_up: DoubleField = DoubleField()
    limit_down: DoubleField = DoubleField()

    open_price: DoubleField = DoubleField()
    high_price: DoubleField = DoubleField()
    low_price: DoubleField = DoubleField()
    pre_close: DoubleField = DoubleField()

    bid_price_1: DoubleField = DoubleField()
    bid_price_2: DoubleField = DoubleField(null=True)
    bid_price_3: DoubleField = DoubleField(null=True)
    bid_price_4: DoubleField = DoubleField(null=True)
    bid_price_5: DoubleField = DoubleField(null=True)

    ask_price_1: DoubleField = DoubleField()
    ask_price_2: DoubleField = DoubleField(null=True)
    ask_price_3: DoubleField = DoubleField(null=True)
    ask_price_4: DoubleField = DoubleField(null=True)
    ask_price_5: DoubleField = DoubleField(null=True)

    bid_volume_1: DoubleField = DoubleField()
    bid_volume_2: DoubleField = DoubleField(null=True)
    bid_volume_3: DoubleField = DoubleField(null=True)
    bid_volume_4: DoubleField = DoubleField(null=True)
    bid_volume_5: DoubleField = DoubleField(null=True)

    ask_volume_1: DoubleField = DoubleField()
    ask_volume_2: DoubleField = DoubleField(null=True)
    ask_volume_3: DoubleField = DoubleField(null=True)
    ask_volume_4: DoubleField = DoubleField(null=True)
    ask_volume_5: DoubleField = DoubleField(null=True)

    localtime: DateTimeField = DateTimeMillisecondField(null=True)

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange", "datetime"), True),)


class DbBarOverview(Model):
    """K线汇总数据表映射对象"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    interval: CharField = CharField()
    count: IntegerField = IntegerField()
    start: DateTimeField = DateTimeField()
    end: DateTimeField = DateTimeField()

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange", "interval"), True),)


class DbTickOverview(Model):
    """Tick汇总数据表映射对象"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    count: IntegerField = IntegerField()
    start: DateTimeField = DateTimeField()
    end: DateTimeField = DateTimeField()

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange"), True),)

"""nifx 20250621"""
class DbSymbolInfo(Model):
    """标的物信息表映射对象"""

    id: AutoField = AutoField()

    symbol = CharField(verbose_name='标的代码')
    sec_type1 = IntegerField(verbose_name='证券品种大类')
    sec_type2 = IntegerField(verbose_name='证券品种细类')
    board = IntegerField(null=True, verbose_name='板块分类')
    exchange = CharField(verbose_name='交易所代码')
    sec_id = CharField(verbose_name='交易所标的代码')
    sec_name = CharField(null=True, verbose_name='交易所标的名称')
    sec_abbr = CharField(null=True, verbose_name='交易所标的简称')
    price_tick = DoubleField(null=True, verbose_name='最小变动单位')
    trade_n = IntegerField(null=True, verbose_name='交易制度')
    listed_date = DateTimeField(null=True, verbose_name='上市日期')
    delisted_date = DateTimeField(null=True, verbose_name='退市日期')
    underlying_symbol = CharField(null=True, verbose_name='标的资产symbol')
    option_type = CharField(null=True, verbose_name='期权行权方式')
    option_margin_ratio1 = DoubleField(null=True, verbose_name='期权保证金计算系数1')
    option_margin_ratio2 = DoubleField(null=True, verbose_name='期权保证金计算系数2')
    call_or_put = CharField(null=True, verbose_name='期权合约类型')
    conversion_start_date = DateTimeField(null=True, verbose_name='可转债开始转股日期')
    delisting_begin_date = DateTimeField(null=True, verbose_name='退市整理开始日')

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange"), True),)
        table_name = 'tsymbolinfo'


class MysqlDatabase(BaseDatabase):
    """Mysql数据库接口"""

    def __init__(self) -> None:
        """"""
        self.db: PeeweeMySQLDatabase = db
        self.db.connect()

        # 如果数据表不存在，则执行创建初始化
        if not DbBarData.table_exists():
            self.db.create_tables([DbBarData, DbTickData, DbBarOverview, DbTickOverview])

        # nifx 20250621如果数据表不存在，则执行创建初始化
        if not DbSymbolInfo.table_exists():
            self.db.create_tables([DbSymbolInfo])

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存K线数据"""
        # 读取主键参数
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval

        # 将BarData数据转换为字典，并调整时区
        data: list = []

        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)

            d: dict = bar.__dict__
            d["exchange"] = d["exchange"].value
            d["interval"] = d["interval"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            if "extra" in d:
                # 如果有extra字段，则删除
                d.pop("extra")
            data.append(d)

        # 使用upsert操作将数据更新到数据库中
        with self.db.atomic():
            for c in chunked(data, 50):
                DbBarData.insert_many(c).on_conflict_replace().execute()

        # 更新K线汇总数据
        overview: DbBarOverview = DbBarOverview.get_or_none(
            DbBarOverview.symbol == symbol,
            DbBarOverview.exchange == exchange.value,
            DbBarOverview.interval == interval.value,
        )

        if not overview:
            overview = DbBarOverview()
            overview.symbol = symbol
            overview.exchange = exchange.value
            overview.interval = interval.value
            overview.start = bars[0].datetime
            overview.end = bars[-1].datetime
            overview.count = len(bars)
        elif stream:
            overview.end = bars[-1].datetime
            overview.count += len(bars)
        else:
            overview.start = min(bars[0].datetime, overview.start)
            overview.end = max(bars[-1].datetime, overview.end)

            s: ModelSelect = DbBarData.select().where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
            )
            overview.count = s.count()

        overview.save()

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        # 读取主键参数
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        # 将TickData数据转换为字典，并调整时区
        data: list = []

        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)

            d: dict = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            d.pop("extra")
            data.append(d)

        # 使用upsert操作将数据更新到数据库中
        with self.db.atomic():
            for c in chunked(data, 50):
                DbTickData.insert_many(c).on_conflict_replace().execute()

        # 更新Tick汇总数据
        overview: DbTickOverview = DbTickOverview.get_or_none(
            DbTickOverview.symbol == symbol,
            DbTickOverview.exchange == exchange.value,
        )

        if not overview:
            overview = DbTickOverview()
            overview.symbol = symbol
            overview.exchange = exchange.value
            overview.start = ticks[0].datetime
            overview.end = ticks[-1].datetime
            overview.count = len(ticks)
        elif stream:
            overview.end = ticks[-1].datetime
            overview.count += len(ticks)
        else:
            overview.start = min(ticks[0].datetime, overview.start)
            overview.end = max(ticks[-1].datetime, overview.end)

            s: ModelSelect = DbTickData.select().where(
                (DbTickData.symbol == symbol)
                & (DbTickData.exchange == exchange.value)
            )
            overview.count = s.count()

        overview.save()

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """"""
        s: ModelSelect = (
            DbBarData.select().where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
                & (DbBarData.datetime >= start)
                & (DbBarData.datetime <= end)
            ).order_by(DbBarData.datetime)
        )

        bars: list[BarData] = []
        for db_bar in s:
            bar: BarData = BarData(
                symbol=db_bar.symbol,
                exchange=Exchange(db_bar.exchange),
                datetime=datetime.fromtimestamp(db_bar.datetime.timestamp(), DB_TZ),
                interval=Interval(db_bar.interval),
                volume=db_bar.volume,
                turnover=db_bar.turnover,
                open_interest=db_bar.open_interest,
                open_price=db_bar.open_price,
                high_price=db_bar.high_price,
                low_price=db_bar.low_price,
                close_price=db_bar.close_price,
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """读取TICK数据"""
        s: ModelSelect = (
            DbTickData.select().where(
                (DbTickData.symbol == symbol)
                & (DbTickData.exchange == exchange.value)
                & (DbTickData.datetime >= start)
                & (DbTickData.datetime <= end)
            ).order_by(DbTickData.datetime)
        )

        ticks: list[TickData] = []
        for db_tick in s:
            tick: TickData = TickData(
                symbol=db_tick.symbol,
                exchange=Exchange(db_tick.exchange),
                datetime=datetime.fromtimestamp(db_tick.datetime.timestamp(), DB_TZ),
                name=db_tick.name,
                volume=db_tick.volume,
                turnover=db_tick.turnover,
                open_interest=db_tick.open_interest,
                last_price=db_tick.last_price,
                last_volume=db_tick.last_volume,
                limit_up=db_tick.limit_up,
                limit_down=db_tick.limit_down,
                open_price=db_tick.open_price,
                high_price=db_tick.high_price,
                low_price=db_tick.low_price,
                pre_close=db_tick.pre_close,
                bid_price_1=db_tick.bid_price_1,
                bid_price_2=db_tick.bid_price_2,
                bid_price_3=db_tick.bid_price_3,
                bid_price_4=db_tick.bid_price_4,
                bid_price_5=db_tick.bid_price_5,
                ask_price_1=db_tick.ask_price_1,
                ask_price_2=db_tick.ask_price_2,
                ask_price_3=db_tick.ask_price_3,
                ask_price_4=db_tick.ask_price_4,
                ask_price_5=db_tick.ask_price_5,
                bid_volume_1=db_tick.bid_volume_1,
                bid_volume_2=db_tick.bid_volume_2,
                bid_volume_3=db_tick.bid_volume_3,
                bid_volume_4=db_tick.bid_volume_4,
                bid_volume_5=db_tick.bid_volume_5,
                ask_volume_1=db_tick.ask_volume_1,
                ask_volume_2=db_tick.ask_volume_2,
                ask_volume_3=db_tick.ask_volume_3,
                ask_volume_4=db_tick.ask_volume_4,
                ask_volume_5=db_tick.ask_volume_5,
                localtime=db_tick.localtime,
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        d: ModelDelete = DbBarData.delete().where(
            (DbBarData.symbol == symbol)
            & (DbBarData.exchange == exchange.value)
            & (DbBarData.interval == interval.value)
        )
        count: int = d.execute()

        # 删除K线汇总数据
        d2: ModelDelete = DbBarOverview.delete().where(
            (DbBarOverview.symbol == symbol)
            & (DbBarOverview.exchange == exchange.value)
            & (DbBarOverview.interval == interval.value)
        )
        d2.execute()
        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        d: ModelDelete = DbTickData.delete().where(
            (DbTickData.symbol == symbol)
            & (DbTickData.exchange == exchange.value)
        )

        count: int = d.execute()

        # 删除Tick汇总数据
        d2: ModelDelete = DbTickOverview.delete().where(
            (DbTickOverview.symbol == symbol)
            & (DbTickOverview.exchange == exchange.value)
        )
        d2.execute()
        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """查询数据库中的K线汇总信息"""
        # 如果已有K线，但缺失汇总信息，则执行初始化
        data_count: int = DbBarData.select().count()
        overview_count: int = DbBarOverview.select().count()
        if data_count and not overview_count:
            self.init_bar_overview()

        s: ModelSelect = DbBarOverview.select()
        overviews: list[BarOverview] = []
        for overview in s:
            overview.exchange = Exchange(overview.exchange)
            overview.interval = Interval(overview.interval)
            overviews.append(overview)
        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """查询数据库中的Tick汇总信息"""
        s: ModelSelect = DbTickOverview.select()
        overviews: list = []
        for overview in s:
            overview.exchange = Exchange(overview.exchange)
            overviews.append(overview)
        return overviews

    def init_bar_overview(self) -> None:
        """初始化数据库中的K线汇总信息"""
        s: ModelSelect = (
            DbBarData.select(
                DbBarData.symbol,
                DbBarData.exchange,
                DbBarData.interval,
                fn.COUNT(DbBarData.id).alias("count")
            ).group_by(
                DbBarData.symbol,
                DbBarData.exchange,
                DbBarData.interval
            )
        )

        for data in s:
            overview: DbBarOverview = DbBarOverview()
            overview.symbol = data.symbol
            overview.exchange = data.exchange
            overview.interval = data.interval
            overview.count = data.count

            start_bar: DbBarData = (
                DbBarData.select()
                .where(
                    (DbBarData.symbol == data.symbol)
                    & (DbBarData.exchange == data.exchange)
                    & (DbBarData.interval == data.interval)
                )
                .order_by(Asc(DbBarData.datetime))
                .first()
            )
            overview.start = start_bar.datetime

            end_bar: DbBarData = (
                DbBarData.select()
                .where(
                    (DbBarData.symbol == data.symbol)
                    & (DbBarData.exchange == data.exchange)
                    & (DbBarData.interval == data.interval)
                )
                .order_by(Desc(DbBarData.datetime))
                .first()
            )
            overview.end = end_bar.datetime

            overview.save()
    
    """nifx 20250621"""
    def save_symbol_info(self, symbol_infos: list[DbSymbolInfo]) -> bool:
        """保存标的物信息"""
        if not symbol_infos:
            return True
                
        # 转换为数据库模型需要的字典格式
        data = []
        for info in symbol_infos:
            info.listed_date = convert_tz(info.listed_date) if info.listed_date else None
            info.delisted_date = convert_tz(info.delisted_date) if info.delisted_date else None
            info.conversion_start_date = convert_tz(info.conversion_start_date) if info.conversion_start_date else None
            info.delisting_begin_date = convert_tz(info.delisting_begin_date) if info.delisting_begin_date else None

            d = {
                "symbol": info.symbol,
                "exchange": info.exchange,
                "sec_type1": info.sec_type1,
                "sec_type2": info.sec_type2,
                "board": info.board,
                "sec_id": info.sec_id,
                "sec_name": info.sec_name,
                "sec_abbr": info.sec_abbr,
                "price_tick": info.price_tick,
                "trade_n": info.trade_n,
                "listed_date": info.listed_date,
                "delisted_date": info.delisted_date,
                "underlying_symbol": info.underlying_symbol,
                "option_type": info.option_type,
                "option_margin_ratio1": info.option_margin_ratio1,
                "option_margin_ratio2": info.option_margin_ratio2,
                "call_or_put": info.call_or_put,
                "conversion_start_date": info.conversion_start_date,
                "delisting_begin_date": info.delisting_begin_date
            }
            data.append(d)
        
        # 批量插入/更新
        with self.db.atomic():
            for batch in chunked(data, 100):  # 每批100条
                DbSymbolInfo.insert_many(batch).on_conflict_replace().execute()
                
        return True

    def load_symbol_info(
        self, 
        symbol: str | None = None,
        exchange: str | None = None
    ) -> list[DbSymbolInfo]:
        """查询标的物信息"""
        query = DbSymbolInfo.select()
        
        # 构建查询条件
        conditions = []
        if symbol:
            conditions.append(DbSymbolInfo.symbol == symbol)
        if exchange:
            conditions.append(DbSymbolInfo.exchange == exchange)
            
        if conditions:
            query = query.where(*conditions)
            
        result = []
        for db_info in query:
            # 转换为SymbolInfo对象
            info = DbSymbolInfo(
                symbol=db_info.symbol,
                sec_type1=db_info.sec_type1,
                sec_type2=db_info.sec_type2,
                exchange=db_info.exchange,
                sec_id=db_info.sec_id,
                board=db_info.board,
                sec_name=db_info.sec_name,
                sec_abbr=db_info.sec_abbr,
                price_tick=db_info.price_tick,
                trade_n=db_info.trade_n,
                listed_date=db_info.listed_date,
                delisted_date=db_info.delisted_date,
                underlying_symbol=db_info.underlying_symbol,
                option_type=db_info.option_type,
                option_margin_ratio1=db_info.option_margin_ratio1,
                option_margin_ratio2=db_info.option_margin_ratio2,
                call_or_put=db_info.call_or_put,
                conversion_start_date=db_info.conversion_start_date,
                delisting_begin_date=db_info.delisting_begin_date
            )
            result.append(info)
            
        return result

    def delete_symbol_info(
        self, 
        symbol: str, 
        exchange: str
    ) -> int:
        """删除标的物信息"""
        if not symbol and not exchange:
            return 0  # 防止误删全部数据
            
        query = DbSymbolInfo.delete()
        
        conditions = []
        if symbol:
            conditions.append(DbSymbolInfo.symbol == symbol)
        if exchange:
            conditions.append(DbSymbolInfo.exchange == exchange)
            
        if conditions:
            query = query.where(*conditions)
            return query.execute()
            
        return 0
