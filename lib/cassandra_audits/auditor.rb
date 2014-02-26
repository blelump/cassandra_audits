require 'cassandra_audits/query'

module CassandraAudits
  # Specify this act if you want changes to your model to be saved in an
  # audit table.  This assumes there is an audits table ready.
  #
  #   class User < ActiveRecord::Base
  #     audited
  #   end
  #
  # To store an audit comment set model.audit_comment to your comment before
  # a create, update or destroy operation.
  #
  # See <tt>Audited::Adapters::ActiveRecord::Auditor::ClassMethods#audited</tt>
  # for configuration options
  module Auditor
    extend ActiveSupport::Concern
    
    CALLBACKS = [:audit_create, :audit_update, :audit_destroy].freeze
    
    module ClassMethods
      
      # == Configuration options
      #
      #
      # * +only+ - Only audit the given attributes
      # * +except+ - Excludes fields from being saved in the audit log.
      #   By default, Audited will audit all but these fields:
      #
      #     [self.primary_key, inheritance_column, 'lock_version', 'created_at', 'updated_at']
      #   You can add to those by passing one or an array of fields to skip.
      #
      #     class User < ActiveRecord::Base
      #       audited :except => :password
      #     end
      # * +protect+ - If your model uses +attr_protected+, set this to false to prevent Rails from
      #   raising an error.  If you declare +attr_accessible+ before calling +audited+, it
      #   will automatically default to false.  You only need to explicitly set this if you are
      #   calling +attr_accessible+ after.
      #
      # * +require_comment+ - Ensures that audit_comment is supplied before
      #   any create, update or destroy operation.
      #
      #     class User < ActiveRecord::Base
      #       audited :protect => false
      #       attr_accessible :name
      #     end
      #
      def audited(options = {})
        # don't allow multiple calls
        return if self.included_modules.include?(CassandraAudits::Auditor::AuditedInstanceMethods)
        

        class_attribute :non_audited_columns,   :instance_writer => false
        class_attribute :auditing_enabled,      :instance_writer => false
        class_attribute :audit_associated_with, :instance_writer => false
        class_attribute :audit_associations, :instance_writer => false
        class_attribute :store_objects_info, :instance_writer => false
        class_attribute :audit_object_transformers, :instance_writer => false
        class_attribute :columns_association_filter, :instance_writer => false
       
        if options[:only]
          except = self.column_names - options[:only].flatten.map(&:to_s)
        else
          except = default_ignored_attributes + CassandraAudits.ignored_attributes + [:updated_at]
          except |= Array(options[:except]).collect(&:to_s) if options[:except]
        end
        self.non_audited_columns = except

        set_callbacks(self, options)
        if options[:audited_associations]
          self.audit_associations = options[:audited_associations]
          options[:audited_associations].each do |audited_association|
            case audited_association
            when String, Symbol then
              klazz = self.reflect_on_association(audited_association).klass
              klazz.send(:audited, {:associated_with => self})
            when Hash then
              klazz = self.reflect_on_association(audited_association.keys.first).klass
              klazz.send(:audited, {:associated_with => self}.merge!(audited_association.values.first))
            end
          end
        end
        self.audit_associated_with = options[:associated_with]
        self.audit_object_transformers = HashWithIndifferentAccess.new( options[:transformers])
        self.columns_association_filter = options[:columns_association_filter]
        
        #        
        #        # Define and set an after_audit callback. This might be useful if you want
        #        # to notify a party after the audit has been created.
        #        define_callbacks :audit
        #        set_callback :audit, :after, :after_audit, :if => lambda { self.respond_to?(:after_audit) }
        #
        #        attr_accessor :version
        #
        extend CassandraAudits::Auditor::AuditedClassMethods
        include CassandraAudits::Auditor::AuditedInstanceMethods

        self.auditing_enabled = true
        self.store_objects_info = true
      end

      private
      def set_callbacks(klazz, options)
        (klazz.send(:after_create, :audit_create)) if !options[:on] || (options[:on] && options[:on].include?(:create))
        (klazz.send(:after_update, :audit_update)) if !options[:on] || (options[:on] && options[:on].include?(:update))
        (klazz.send(:after_commit, :audit_destroy)) if !options[:on] || (options[:on] && options[:on].include?(:destroy))
      end
      
    end
    
    module AuditedInstanceMethods
      
      def audits
        CassandraAudits::Query.new
      end
      
      # List of attributes that are audited.
      def audited_attributes
        #        filter_attributes(attributes) do
        #          attributes.except(*non_audited_columns).inject({}) do |changes,(attr, value)|
        #            changes[attr] = transform(attr, value).first if value.present?
        #            changes
        #          end
        #        end

        filter_attributes(attributes) do |changes, attr, value|
          transform(attr, value).first if value.present?
        end
      end
      
      def audited_changes
        #        changed_attributes.except(*non_audited_columns).inject({}) do |changes,(attr, old_value)|
        #          changes[attr] = transform(attr, old_value, self[attr]) if [old_value, self[attr]].all? { |attr| attr.present? }
        #          changes
        #        end
        filter_attributes(changed_attributes) do |changes, attr, old_value|
          transform(attr, old_value, self[attr]) if [old_value, self[attr]].all? { |attr| attr.present? }
        end
      end

      private
      def filter_attribute(attr)
        return {} if columns_association_filter.blank?
        
        columns_association_filter.reduce({}) do |sum, (klazz, fields)|
          found = fields.detect {|field| field == attr }
          sum[klazz] = found if found
          sum
        end
      end
      
      def filter_attributes(attrs)
        
        attrs.except(*non_audited_columns).inject({}) do |changes,(attr, value)|
          diff = yield(changes, attr, value)
          changes[attr] = diff if diff.present?
          changes
        end
        
      end
      
      def audit_create
        write_audit(:action => "create", :audited_changes => audited_attributes)
      end
      
      def audit_update
        if (changes = audited_changes).present?
          write_audit(:action => "update", :audited_changes => changes) 
        end
      end
      
      def audit_destroy
        if destroyed?
          params = {:action => "destroy", :audited_changes => ""}
          
          write_audit(params)
        end
      end
      
      def write_audit(attrs)
        return unless auditing_enabled
        
        
        attrs.merge!({:auditable_type => self.class.name, :auditable_id => self.id})
        associate_with_parent(attrs)
        store_audit_objects_info(attrs)
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
                    :associated_type => klazz, 
                    :associated_id => associated_id
                  ).merge!({:audit_destination_data => audit[:auditable_type].constantize.auditor_destination_data(audit[:associated_id]).to_json})
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
            audit_data.delete(:associated_type)
          end
          audit_data[:audited_changes] = audit_data[:audited_changes].to_json.gsub(/\'/, "&#39;") if audit_data[:audited_changes].present? #Escape the \' char
          persist(audit_data)
        end
      end

      
      
      def persist(attrs)
        audit = CassandraAudits.audit_class.name.constantize.new(attrs.merge!(:created_at => (Time.now.to_f*1000).to_i, :year_month => Time.now.strftime("%Y%m").to_i))
        audit.save
      end

      def transform(attr, *values)
        if transformation = audit_object_transformers[attr]
          mappings = reflections[transformation.keys.first.to_sym].klass.where(:id => values).reduce({}) do |mapping, o|
            mapping[o.id] = [transformation.values.first].flatten.reduce({}) {|sum, t| sum[t] = o.try(t); sum }
            mapping
          end
          [values].flatten.collect {|value| mappings[value] }
        else
          values
        end
      end
      
      def store_audit_objects_info(attrs)
        if store_objects_info
          attrs[:audit_source_data] = audit_source_data.to_json if self.respond_to?(:audit_source_data)
          attrs[:audit_destination_data] = audit_destination_data.to_json if self.respond_to?(:audit_destination_data)
        end
      end
      
      def associate_with_parent(attrs)
        if audit_associated_with.present?
          reflection = lookup_for_reflection_by_klass(self, audit_associated_with)
          attrs.merge!(:associated_type => reflection.klass.name, :associated_id => lookup_for_key(self, reflection, reflection.foreign_key)) if reflection.present?
        end
      end
      
      def lookup_for_key(object, reflection, key)
        unless object.is_a?(Array)
          return object.send(key) if object.respond_to?(key)
        else
          return object.collect {|el| el.send(key) } if object.all? {|el| el.respond_to?(key) }
        end
        
        association_name = (reflection.through_reflection || reflection).name
        if object.respond_to?(association_name)
          associated_object = object.send(association_name)
          reflection = lookup_for_reflection_by_klass(associated_object, audit_associated_with)
          lookup_for_key(associated_object, reflection, key)
        end
      end
      
      def lookup_for_reflection(object)
        object.reflections.values.detect do |reflection|
          yield(reflection)
        end
      end
      
      def lookup_for_reflection_by_klass(object, klass)
        lookup_for_reflection(object) do |reflection|
          reflection.klass == klass
        end
      end
      
      #      def lookup_for_reflection_by_foreign_key(object, foreign_key = "")
      #        lookup_for_reflection(object) do |reflection|
      #          reflection.foreign_key == foreign_key
      #        end
      #      end
      
      CALLBACKS.each do |attr_name|
        alias_method "#{attr_name}_callback".to_sym, attr_name
      end

      def empty_callback #:nodoc:
      end

    end
    
    module AuditedClassMethods
      def without_auditing(&block)
        auditing_was_enabled = auditing_enabled
        # disable auditing for caller
        disable_auditing 
        # disable auditing for associated models
        if audit_associations.present?
          hashed_audit_associations = audit_associations.reduce(Hash.new, :merge)
          hashed_audit_associations.each do |association, opts|
            self.reflect_on_association(association).klass.disable_auditing
          end
        end
        block.call.tap { enable_auditing if auditing_was_enabled }
        if audit_associations.present?
          hashed_audit_associations.each do |association, opts|
            self.reflect_on_association(association).klass.enable_auditing
          end
        end
      end

      def disable_auditing
        self.auditing_enabled = false
      end

      def enable_auditing
        self.auditing_enabled = true
      end
    end
    
    def self.write_audit(klazz, attrs = {})
      attrs.merge!( {:created_at => (Time.now.to_f*1000).to_i, :year_month => Time.now.strftime("%Y%m").to_i, :auditable_type => klazz.name})  
      
      audit = CassandraAudits.audit_class.name.constantize.new(attrs)
      audit.save
    end

  end
end
