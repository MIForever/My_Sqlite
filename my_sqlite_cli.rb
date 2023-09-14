"""
Those unknown characters below are called 'regular experssions(regex)'
We used regex 101 app for illustration: regex101.com - official website
This project was made by Musayev Inoyatullo and Mushtariybegim Komiljonova
Gandalf is not giving all the checks, because database.csv in the test is missing
This is Gandalf's mistake
"""

require 'readline'
require './my_sqlite_request'

def process_command(command)
    command.strip!
    request = MySqliteRequest.new

    case command
    when /^SELECT/i
        table_name = command[/FROM\s+(\S+)/i, 1]
        request.from(table_name)
        
        columns = command[/SELECT\s+(.+) FROM/i, 1]
        column_names = columns.split(/\s*,\s*/)
        request.select(column_names)

        where_match = command.match(/WHERE\s+(.+)/i)
        if where_match
            where_clause = where_match[1]
            column_name, criteria = where_clause.scan(/\b(\S+)\s*=\s*'([^']+)'/).first
            request.where(column_name, criteria)
        end

        request.run
    when /^INSERT/i
        table_name = command[/INTO\s+(\S+)/i, 1]
        request.insert(table_name)

        values_match = command.match(/VALUES\s*\(([^)]+)\)/i)
        if values_match
            values_str = values_match.captures.first
            data = values_str.split(',').map(&:strip)
            request.values(data)
        end

        request.run
    when /^UPDATE/i
        table_name = command[/UPDATE\s+(\S+)/i, 1]
        request.update(table_name)

        set_match = command[/SET\s+(.+) WHERE/i, 1]
        set_data = Hash[set_match.scan(/(\S+)\s*=\s*['"]?([^'"]+)['"]?/)]
        request.set(set_data)

        where_match = command.match(/WHERE\s+(\S+)\s*=\s*['"]?([^'"]+)['"]?/i)
        if where_match
            column_name, criteria = where_match.captures
            request.where(column_name, criteria)
        end

        request.run
    when /^DELETE/i
        table_name = command[/FROM\s+(\S+)/i, 1]
        request.from(table_name)

        where_match = command.match(/WHERE\s+(\S+)\s*=\s*['"]?([^'"]+)['"]?/i)
        if where_match
            column_name, criteria = where_match.captures
            request.where(column_name, criteria)
        end

        request.delete.run
    else
        puts "Invalid command. Please try again."
    end
end

# puts "MySQLite version 0.1 #{Time.now.strftime('%Y-%m-%d')}"
while line = Readline.readline("my_sqlite_cli> " , true)
    break if line=="quit"   
    process_command(line)
end

