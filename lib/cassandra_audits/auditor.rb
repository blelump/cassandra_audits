require 'cassandra_audits/query'
require 'cassandra_audits/worker'

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

      def audits(model)
        CassandraAudits::Query.new("#{model}_audits")
      end

      # List of attributes that are audited.
      def audited_attributes

        filter_attributes(attributes) do |changes, attr, value|
          transform(attr, value).first if value.present?
        end
      end

      def audited_changes
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

          params = {:action => "destroy", :audited_changes => audited_attributes}

          write_audit(params)

        end
      end

      def write_audit(attrs)
        return unless auditing_enabled

        attrs.merge!({:auditable_type => self.class.name, :auditable_id => self.id})
        data = CassandraAudits.audit_scope.const_get("UserInfo").new
        data = data.take
        associate_with_parent(attrs)

        Resque.enqueue(CassandraAudits::Worker, attrs.merge(data),
                       Usi::Year.current_year.to_short, I18n.locale)


      end

      def associate_with_parent(attrs)
        if audit_associated_with.present?
          reflection = lookup_for_reflection_by_klass(self, audit_associated_with)
          if reflection.present?
            attrs.merge!(:klazz => reflection.klass.name,
                         :associated_id => lookup_for_key(self, reflection, reflection.foreign_key))
          end
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

      def transform(attr, *values)
        if transformation = audit_object_transformers[attr]
          # mappings = reflections[transformation.keys.first.to_sym].klass
          # .where(:id => values)
          mappings = [send(transformation.keys.first.to_sym)].flatten
          .reduce({}) do |mapping, o|
            mapping[o.id] = [transformation.values.first].flatten.reduce({}) {|sum, t| sum[t] = o.try(t); sum }
            mapping
          end
          [values].flatten.collect {|value| mappings[value] }
        else
          values
        end
      end

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
      attrs.merge!( {:created_at => (Time.now.to_f*1000).to_i, :auditable_type => klazz.name})

      data = CassandraAudits.audit_scope.const_get("UserInfo").new(attrs)
      data = data.take
      attrs.merge!(data)
      audit = CassandraAudits.audit_scope
      .const_get("#{attrs.delete(:klazz).demodulize}#{CassandraAudits.audit_class.name.demodulize}")
      .new(attrs)
      audit.save
    end

  end
end
