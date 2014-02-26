module CassandraAudits
  class Query 
    class LastWithoutLimitNotSupported < ::Exception; end
      
    attr_accessor :selection_values, :limit_value, :order_values
    attr_reader :loaded
    alias :loaded? :loaded
      
    def initialize
      @loaded = false
    end
    
    def limit(value)
      relation = clone
      relation.limit_value = value
      relation
    end
      
    def order(args)
      return self if args.blank?

      relation = clone
      relation.order_values = args
      relation
    end
    
    def where(opts)
      return self if opts.blank?

      relation = clone
      relation.selection_values = build_where(opts)
      relation
    end
      
    def first
      find_first
    end
      
    def last
      find_last
    end
      
    def inspect
      to_a.inspect
    end
      
    def to_a
      return @records if loaded?
      @records = CassandraMigrations::Cassandra
      .select(:audits, build_query).to_a
      .collect {|data| CassandraAudits.audit_class.name.constantize.new(data, true) } 
      @loaded = true      
      @records
    end
      
    def reload
      reset
      to_a
      self
    end

    def reset
      @loaded = @first = @last = @allow_filtering = nil
      @records = []
      self
    end
      
    def allow_filtering
      @allow_filtering = true
    end
    
    private
    def build_query
      
      {:selection => selection_values, :limit => "#{limit_value} #{"ALLOW FILTERING" if @allow_filtering.present?}", :order_by => order_values}.reduce({}) do |sum, (k,v)|
        sum[k] = v if v.present?
        sum
      end.tap do |query|
        Rails.logger.debug "\e[1;35m [Cassandra Audits] \e[0m #{query.to_s}"
      end
    end
    
    def find_first
      if loaded?
        @records.first
      else
        @first ||= limit(1).to_a[0]
      end
    end

    def find_last
      if loaded?
        @records.last
      else
        @last ||=
          if limit_value
          to_a.last
        else
          raise LastWithoutLimitNotSupported
        end
      end
    end

      
    def build_from_hash(attributes)
      selection = attributes.collect do |key, value|
        "#{key} = #{value}"
      end
      selection.join(" AND ")
    end
      
    def build_where(opts)
      self.selection_values = "" if selection_values.nil?
      self.selection_values += case opts
      when Hash
        build_from_hash(opts)
      when String
        opts
      end
    end
    
    
      
  end
end