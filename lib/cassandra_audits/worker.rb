class CassandraAudits::Worker
  @queue = :cassandra_audits



  def self.perform(attrs, short_annual, locale)


    # CassandraMigrations::Cassandra.use "#{Rails.env}#{short_annual}"
    configs = YAML.load(ERB.new(File.new(Rails.root.join("config", "cassandra.yml")).read).result)
    $client.use configs["#{Rails.env}#{short_annual}"]['keyspace']
    CassandraMigrations::Cassandra.client = $client

    runner = Inner.new(attrs, short_annual, locale)
    runner.run

  end

  private

  class Inner

    attr_reader :klazz, :context, :short_annual, :attrs, :decorator, :locale

    extend Forwardable

    def_delegators :@klazz, :audit_associated_with, :columns_association_filter
    def_delegators :@decorator, :audit_source_data, :audit_destination_data

    def initialize(attrs, short_annual, locale)
      @klazz = attrs['auditable_type'].constantize
      @attrs = ActiveSupport::HashWithIndifferentAccess.new(attrs)
      @short_annual = short_annual
      @locale = locale
    end

    def run

      ActiveRecord.within_annual(short_annual) do

        if destroy?
          @context = klazz.new
        else
          @context = klazz.where(:id => attrs['auditable_id']).first
        end
        @decorator = Audit.const_get("#{attrs['auditable_type'].demodulize}Decorator")
        .new(context,
             :current_year => Usi::Year.from_short(short_annual),
             :current_language => locale)


        return if context.blank?

        begin
          unless destroy?
            store_audit_objects_info(attrs)
          else
            attrs[:klazz] = klazz.audit_associated_with.try(:name)
          end
          audits = [attrs]

          if attrs[:associated_id].present?
            associated_ids = [attrs.delete(:associated_id)].flatten.uniq
            audits = audits.reduce([]) do |sum, audit|
              associated_ids.each do |id|
                sum << audit.merge(:associated_id => id)
              end
              sum
            end
          end
          if attrs[:audited_changes].present?
            if columns_association_filter.present?
              filtered = {}
              columns_association_filter.each do |klazz, fields|
                filtered[klazz] = {}
              end
              attrs[:audited_changes].each do |attr, data|
                if filter = filter_attribute(attr.to_sym)
                  filter.each do |klazz, field|
                    filtered[klazz][attr] = data
                  end
                end
              end
              if filtered.any? {|k,v| v.present?  }
                
                audits = audits.reduce([]) do |sum, audit|
                  filtered.each do |klazz, fields|
                    associated_id = klazz == audit[:auditable_type] ? audit[:auditable_id] : audit[:associated_id]
                    next if fields.blank? || associated_id.blank?
                    sum << audit.merge(
                      :audited_changes => fields,
                      :klazz => klazz,
                      :associated_id => associated_id
                    )
                    .merge!({:audit_destination_data => decorator.auditor_destination_data(audit[:associated_id]).to_json})
                  end
                  sum
                end
              end
            end
          else
            attrs.delete(:audited_changes)
          end

          audits.each do |audit_data|
            if audit_data[:associated_id].blank?
              audit_data.delete(:associated_id)
            end
            if audit_data[:audited_changes].present? #Escape the \' char
              audit_data[:audited_changes] = audit_data[:audited_changes]
              .to_json.gsub(/\'/, "&#39;")
            end
          end

          audits += audits.collect do |audit|
            a = audit.dup
            a[:klazz] = CassandraAudits.current_user_class
            a
          end

          if decorator.respond_to?(:associate_nested)
            decorator.associate_nested(audits)
          end
          puts audits
          audits.each {|audit| persist(audit)  }

        rescue Exception => e
          puts e.message
          puts e.backtrace
        end
        return
      end

    end


    private

    def persist(attrs)
      audit = CassandraAudits.audit_scope.const_get("#{attrs.delete(:klazz).demodulize}#{CassandraAudits.audit_class.name.demodulize}").new(attrs.merge!(:created_at => (Time.now.to_f*1000).to_i))
      audit.save
    end

    def destroy?
      attrs['action'] == 'destroy'
    end

    def store_audit_objects_info(attrs)
      if decorator.respond_to?(:audit_source_data)
        attrs[:audit_source_data] = audit_source_data.to_json
      end
      if decorator.respond_to?(:audit_destination_data)
        attrs[:audit_destination_data] = audit_destination_data.to_json
      end
    end

    def filter_attribute(attr)
      return {} if columns_association_filter.blank?

      columns_association_filter.reduce({}) do |sum, (klazz, fields)|
        found = fields.detect {|field| field == attr }
        sum[klazz] = found if found
        sum
      end
    end

  end






end
