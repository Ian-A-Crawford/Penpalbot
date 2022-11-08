import logging
import os
from datetime import datetime
from telegram import Update, User
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes
import sqlite3
from sqlite3 import Error
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)



def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)
    return conn

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Welcome to the penpal bot! Try /addme to be added to the penpal queue!")


async def addme(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = create_connection(r"pythonsqlite.db")
    user = update.message.from_user
    print(user)
    if conn is None or user['is_bot']:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="You must specify blah blah blah")
    else:
        user = (user['username'], user['id'], int(datetime.utcnow().timestamp()), 0)
        hm = await insert_into(conn, user)
        if hm == True:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="You have been added to the queue!")
            await findmatch(user, context)
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="You are already in the queue!")


def create_table(conn, startup_cmd):
    try:
        c = conn.cursor()
        c.execute(startup_cmd)
    except Error as e:
        print(e)


async def insert_into(conn, user):
    cmd = """INSERT INTO users(name, telegramid, timestamp, matched)
    VALUES(?,?,?,?)"""
    cur = conn.cursor()
    time = int(datetime.utcnow().timestamp() - 259200)

    cur.execute("DELETE FROM users WHERE timestamp < ?", (time,))
    removals = cur.fetchall()
    cur.execute("SELECT name, matched FROM users WHERE name = ?", (user[0],))
    data = cur.fetchone()


    if data:
        return False
    else:
        cur.execute(cmd, user)
        conn.commit()
        return True


async def findmatch(user, context):
    cur = conn.cursor()
    # cur.execute("SELECT * FROM users WHERE matched != 1 ORDER BY timestamp ASC LIMIT 1")
    cmd = """SELECT * FROM users 
    WHERE matched != 1 AND name != ?
    ORDER BY timestamp ASC LIMIT 1"""
    cur.execute(cmd, (user[0],))
    match = cur.fetchone()
    if match:
        print('match: ', match)

        cmd = f"""WITH us AS (
        SELECT * FROM users 
        WHERE matched != 1
        ORDER BY timestamp ASC LIMIT 1
    )
        UPDATE users SET matched = 1 WHERE id IN (SELECT id FROM us) or name = \"{user[0]}\";"""
        cur.execute(cmd)
        await context.bot.send_message(chat_id=user[1], text="Match found! Your match is @" + str(match[1]) + "!")
        await context.bot.send_message(chat_id=match[2], text="Match found! Your match is @" + str(user[0]) + "!")
    else:
        await context.bot.send_message(chat_id=user[1], text="No matches at the moment")



if __name__ == '__main__':
    key = os.environ.get('Pen')
    conn = create_connection(r"pythonsqlite.db")
    application = ApplicationBuilder().token(key).build()


    startup_cmd =  """CREATE TABLE IF NOT EXISTS users(
     id integer PRIMARY KEY,
     name text NOT NULL,
     telegramid integer NOT NULL,
     timestamp integer NOT NULL,
     matched boolean NOT NULL)"""
    if conn is not None:
        create_table(conn, startup_cmd)
    else:
        print("Cannot make database connection")

    start_handler = CommandHandler('start', start)
    addme_handler = CommandHandler('addme', addme)

    application.add_handler(start_handler)
    application.add_handler(addme_handler)

    application.run_polling()