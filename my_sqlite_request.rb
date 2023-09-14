"""
This project was made by Musayev Inoyatullo and Mushtariybegim Komiljonova
Gandalf is not giving all the checks, because database.csv in the test is missing
This is Gandalf's mistake
"""

require 'csv'

class MySqliteRequest
    def initialize
        @table_name = nil
        @join_table = nil
        @order_col = nil
        @order_dir = nil
        @request_type = nil
        @select_columns = []
        @where_conditions = {}
        @update_data = {}
        @insert_data = {}
    end

    def from(table_name)
        @table_name = table_name
        return self
    end
    
    def self.from(table_name)
        request = new
        request.from(table_name)

    end

    def insert(table_name)
        @table_name = table_name
        @request_type = :insert
        return self
    end
    
    def self.insert(table_name)
        request = new
        request.insert(table_name)
    end

    def update(table_name)
        @table_name = table_name
        @request_type = :update
        return self
    end

    def self.update(table_name)
        request = new
        request.update(table_name)
    end 

    def select(*column_names)
        @select_columns.concat(column_names.flatten)
        @request_type = :select
        return self
    end
    
    def where(column_name, criteria)
        @where_conditions[column_name] = criteria
        return self
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b)
        @join_table = { column_on_db_a: column_on_db_a, filename_db_b: filename_db_b, column_on_db_b: column_on_db_b }
        return self
    end

    def order(order, column_name)
        @order_col = column_name
        @order_dir = order
        return self
    end

    def values(data)
        case @request_type
        when :insert
          @insert_data = data
        when :update
          @update_data = data
        end
        return self
    end

    def set(data)
        @update_data = data
        return self
    end

    def delete
        @request_type = :delete
        return self
    end

    def run
    
        if !@table_name.empty? && @request_type.nil?
            save_data({})
        
        else
            data = load_data(@table_name)

            if !data.empty?
                case @request_type
                when :select
                    if @join_table
                        data = join_data(data)
                    end

                    if !@where_conditions.empty?
                        data = filter_data(data)
                    end

                    if @order_col && @order_dir
                        data = sort_data(data)
                    end

                    if !@select_columns.empty?
                        data = select_columns(data)
                        puts data
                    end

                when :insert
                    if @table_name && !@insert_data.empty?
                        data = insert_data(data)
                    end

                when :update
                    if @table_name && !@update_data.empty?
                        data = update_data(data)
                    end
                    
                when :delete
                    if !@where_conditions.empty?
                        data = delete_data(data)
                    end
                end
            end
        end
        reset
        return data
    end

    private

    def reset
        @table_name = nil
        @join_table = nil
        @order_col = nil
        @order_dir = nil
        @request_type = nil
        @select_columns = []
        @where_conditions = {}
        @update_data = {}
        @insert_data = {}
    end

    def load_data(table_name)
        begin
            CSV.read(table_name, headers: true).map(&:to_h)
        rescue Errno::ENOENT
            # puts "Error: File '#{table_name}' not found."
            return []
        end
    end
    
    def join_data(data)
        joined_data = load_data(@join_table[:filename_db_b])
        data.merge(joined_data, on: @join_table[:column_on_db_b], suffix: "_#{@join_table[:filename_db_b]}")
    end

    def filter_data(data)
        data.select do |row|
            @where_conditions.all? { |column_name, criteria| row[column_name.to_s] == criteria }
        end
    end

    def sort_data(data)
        data.sort_by { |row| row[@order_col] }.tap do |sorted_data|
            sorted_data.reverse! if @order_dir == :desc
        end
    end

    def select_columns(data)
        if @select_columns.empty? || @select_columns.include?('*')
            return data
        else
            return data.map { |row| row.select { |column, _| @select_columns.include?(column) } }
        end
    end

    def insert_data(data)
        data << @insert_data
        save_data(data)
    end

    def update_data(data)
        data.each do |row|
            if @where_conditions.all? { |column_name, criteria| row[column_name.to_s] == criteria }
                @update_data.each { |column, value| row[column.to_s] = value }
            end
        end
        save_data(data)
    end

    def delete_data(data)
        data.reject! do |row|
            @where_conditions.all? { |column_name, criteria| row[column_name.to_s] == criteria }
        end
        save_data(data)
    end

    def save_data(data)
        headers = data.empty? ? [] : data.first.keys
        CSV.open(@table_name, 'w', headers: headers, write_headers: true) do |csv|
            data.each { |row| csv << row }
        end
    end
end
