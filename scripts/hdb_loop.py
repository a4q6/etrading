from etr.core.hdb_dumper import HdbDumper

if __name__ == '__main__':
    HdbDumper().start_hdb_loop(n_days=20, notification=True)
    # HdbDumper().dump_to_hdb("data/tp/TP-BitBankSocketClient-BTCJPY.log.2025-06-10")
